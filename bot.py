import os
import logging
import asyncio
import aiohttp
import time
import signal
import sys
import json
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)
from telegram.error import NetworkError, TimedOut, RetryAfter

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
SUBSCRIBERS_FILE = "subscribers.json"
MP3QURAN_API = "https://mp3quran.net/api/v3"


def load_subscribers() -> set:
    if os.path.exists(SUBSCRIBERS_FILE):
        try:
            with open(SUBSCRIBERS_FILE, "r") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()


def save_subscribers(subs: set):
    with open(SUBSCRIBERS_FILE, "w") as f:
        json.dump(list(subs), f)


DAILY_AYAT = [
    {"surah": "البقرة", "ayah": 286, "text": "﴿لَا يُكَلِّفُ اللَّهُ نَفْسًا إِلَّا وُسْعَهَا﴾", "tafseer": "الله لا يُكلّف أحداً فوق طاقته، فكل ما ابتُليتَ به فأنت قادر على تحمّله."},
    {"surah": "آل عمران", "ayah": 173, "text": "﴿حَسْبُنَا اللَّهُ وَنِعْمَ الْوَكِيلُ﴾", "tafseer": "يكفينا الله ونِعم المُعين هو، قالها إبراهيم في النار، وقالها محمد ﷺ يوم أُحُد."},
    {"surah": "الضحى", "ayah": 5, "text": "﴿وَلَسَوْفَ يُعْطِيكَ رَبُّكَ فَتَرْضَىٰ﴾", "tafseer": "وعد رباني بأن العطاء قادم لا محالة حتى تُبلغ الرضا الكامل."},
    {"surah": "الشرح", "ayah": "5-6", "text": "﴿فَإِنَّ مَعَ الْعُسْرِ يُسْرًا ۝ إِنَّ مَعَ الْعُسْرِ يُسْرًا﴾", "tafseer": "كرّر الله اليُسر مرتين مع عُسر واحد، فالفرج أقرب مما تظن."},
    {"surah": "الطلاق", "ayah": 3, "text": "﴿وَمَن يَتَوَكَّلْ عَلَى اللَّهِ فَهُوَ حَسْبُهُ﴾", "tafseer": "من فوّض أمره لله كفاه الله كل شيء، فالتوكل مفتاح الراحة."},
    {"surah": "البقرة", "ayah": 152, "text": "﴿فَاذْكُرُونِي أَذْكُرْكُمْ وَاشْكُرُوا لِي وَلَا تَكْفُرُونِ﴾", "tafseer": "اذكر الله يذكرك، واشكره يزدك. الذكر والشكر مفتاحا الفرج."},
    {"surah": "الزمر", "ayah": 53, "text": "﴿لَا تَقْنَطُوا مِن رَّحْمَةِ اللَّهِ﴾", "tafseer": "مهما بلغت ذنوبك، رحمة الله أوسع وأعظم، فلا تيأس أبدًا."},
    {"surah": "إبراهيم", "ayah": 7, "text": "﴿لَئِن شَكَرْتُمْ لَأَزِيدَنَّكُمْ﴾", "tafseer": "الشكر يضاعف النعم، فكلما شكرت ازددت خيراً ونعمةً."},
    {"surah": "الأنفال", "ayah": 2, "text": "﴿إِنَّمَا الْمُؤْمِنُونَ الَّذِينَ إِذَا ذُكِرَ اللَّهُ وَجِلَتْ قُلُوبُهُمْ﴾", "tafseer": "علامة الإيمان الحقيقي أن يخشع القلب عند ذكر الله."},
    {"surah": "الحجرات", "ayah": 13, "text": "﴿إِنَّ أَكْرَمَكُمْ عِندَ اللَّهِ أَتْقَاكُمْ﴾", "tafseer": "الكرامة عند الله بالتقوى لا بالمال ولا بالنسب."},
    {"surah": "الرعد", "ayah": 28, "text": "﴿أَلَا بِذِكْرِ اللَّهِ تَطْمَئِنُّ الْقُلُوبُ﴾", "tafseer": "العلاج الوحيد لقلق القلوب وضيقها هو ذكر الله."},
    {"surah": "يوسف", "ayah": 87, "text": "﴿لَا تَيْأَسُوا مِن رَّوْحِ اللَّهِ﴾", "tafseer": "لا تفقد الأمل بفرج الله، فإنه يفرج ما لا يخطر على البال."},
    {"surah": "الفجر", "ayah": "27-28", "text": "﴿يَا أَيَّتُهَا النَّفْسُ الْمُطْمَئِنَّةُ ارْجِعِي إِلَىٰ رَبِّكِ رَاضِيَةً مَّرْضِيَّةً﴾", "tafseer": "النهاية المثلى للمؤمن: نفس راضية مرضية تعود إلى ربها."},
    {"surah": "النور", "ayah": 35, "text": "﴿اللَّهُ نُورُ السَّمَاوَاتِ وَالْأَرْضِ﴾", "tafseer": "الله هو النور الذي يُضيء كل شيء، فاستنر بهداه."},
    {"surah": "البقرة", "ayah": 45, "text": "﴿وَاسْتَعِينُوا بِالصَّبْرِ وَالصَّلَاةِ﴾", "tafseer": "الصبر والصلاة سلاحان لا يُهزم من تمسّك بهما."},
]

DUA_ANDALUSIYA = (
    "🤲 *دعاء للأخت الأندلسية*\n"
    "اللهم تقبّل من الأخت الأندلسية هذا البوت ونشر القرآن الكريم صدقةً جاريةً،\n"
    "واجعله في ميزان حسناتها يوم القيامة.\n"
    "اللهم آنسْ وحشتها وأجزل لها المثوبة على صبرها الجميل\n"
    "على غياب زوجها في أرض جزيرة نبيّك محمد ﷺ،\n"
    "واجمع شملهما قريباً على خير وعافية.\n"
    "آمين يا رب العالمين 🌙"
)

WOMEN_TOPICS = {
    "فقه_الطهارة": {
        "title": "📖 فقه الطهارة للمرأة",
        "content": (
            "🌸 *فقه الطهارة للمرأة المسلمة*\n\n"
            "• الحيض: هو دم طبيعي يخرج من رحم المرأة في أوقات معلومة.\n"
            "• المدة: من يوم إلى خمسة عشر يومًا، والغالب ستة أو سبعة أيام.\n"
            "• ما يحرم بالحيض: الصلاة، الصيام، الطواف، قراءة المصحف بالمسّ.\n"
            "• النفاس: دم يخرج بعد الولادة، ومدته أقصاها أربعون يومًا.\n"
            "• الاستحاضة: دم مرض وعلة، لا تُمنع معه الصلاة.\n\n"
            "📌 *مسائل مهمة:*\n"
            "• إذا انقطع الحيض وجب الغسل قبل الصلاة.\n"
            "• يجوز للحائض قراءة القرآن من الحفظ عند كثير من العلماء.\n"
            "• يجوز للحائض الاستماع للقرآن.\n"
        )
    },
    "الحجاب": {
        "title": "👗 أحكام الحجاب",
        "content": (
            "🌸 *أحكام الحجاب الشرعي*\n\n"
            "• الحجاب فريضة واجبة بالكتاب والسنة والإجماع.\n"
            "• شروطه: يغطي جميع البدن ما عدا الوجه والكفين على الراجح.\n"
            "• أن يكون فضفاضًا غير ضيق، غير شفاف، غير مزين.\n"
            "• لا يكون لباس شهرة أو تشبهًا بالرجال.\n\n"
            "📌 *فضل الحجاب:*\n"
            "• صون للمرأة وحماية لها وكرامة.\n"
            "• دليل الإيمان والحياء والعفة.\n"
            "• طاعة لله ورسوله ﷺ.\n"
        )
    },
    "العبادة": {
        "title": "🙏 العبادات الخاصة بالمرأة",
        "content": (
            "🌸 *العبادات وأحكامها للمرأة*\n\n"
            "• صلاة المرأة في بيتها أفضل من صلاتها في المسجد.\n"
            "• يجوز لها صلاة الجماعة في المسجد مع الحجاب الكامل.\n"
            "• تقضي الصيام الذي فاتها بسبب الحيض أو النفاس.\n"
            "• المرأة الحامل والمرضع تفطر وتقضي ولا كفارة عليها.\n"
            "• يستحب للمرأة الإكثار من الذكر والدعاء والصدقة.\n\n"
            "📌 *أوقات الفضل:*\n"
            "• الثلث الأخير من الليل.\n"
            "• بين الأذان والإقامة.\n"
            "• يوم الجمعة خاصة الساعة الأخيرة.\n"
        )
    },
    "الزواج_والأسرة": {
        "title": "💍 الزواج والأسرة",
        "content": (
            "🌸 *أحكام الزواج والأسرة*\n\n"
            "• الزواج سنة مؤكدة وفريضة على القادر الخائف على نفسه.\n"
            "• ركان النكاح: الإيجاب والقبول والولي والشهود والمهر.\n"
            "• حق المرأة في الاختيار وعدم الإكراه.\n"
            "• المهر حق خالص للمرأة لا يؤخذ منها إلا بطيب نفس.\n"
            "• حق النفقة والسكن على الزوج.\n\n"
            "📌 *حقوق الزوجة:*\n"
            "• النفقة والكسوة والسكن.\n"
            "• المعاملة الحسنة والعشرة بالمعروف.\n"
            "• عدم الإضرار بها.\n"
        )
    },
    "أخلاق_إسلامية": {
        "title": "✨ أخلاق المرأة المسلمة",
        "content": (
            "🌸 *أخلاق المرأة المسلمة*\n\n"
            "• الحياء: قال ﷺ (الحياء شعبة من الإيمان).\n"
            "• الصدق في القول والعمل.\n"
            "• الأمانة وحفظ الأسرار.\n"
            "• الرفق واللين في المعاملة.\n"
            "• بر الوالدين وصلة الرحم.\n"
            "• الصبر على البلاء والشكر على النعماء.\n\n"
            "📌 *نساء من الجنة:*\n"
            "قال ﷺ: (خير نساء العالمين: مريم بنت عمران، وخديجة بنت خويلد، وفاطمة بنت محمد، وآسية امرأة فرعون).\n"
        )
    }
}

SAHABA_STORIES = [
    {
        "name": "خبيب بن عدي رضي الله عنه",
        "story": (
            "🌟 *خبيب بن عدي - الفداء بالدم*\n\n"
            "أُسر خبيب بن عدي رضي الله عنه وأُخذ إلى مكة ليُقتل، "
            "فطلب منهم أن يُمكّنوه من ركعتين قبل قتله، فصلى ركعتين في خشوع تام، "
            "ثم قال: (لولا أن تظنوا أني طوّلت جزعًا لزدت).\n\n"
            "وحين رُفع على خشبة الصلب قالوا له: أتود أن محمدًا مكانك؟ "
            "فقال: (لا والله، ما أحب أن يُشاك رسول الله ﷺ بشوكة وأنا جالس في بيتي).\n\n"
            "💫 *الدرس:* حب الصحابة لرسول الله ﷺ كان أعمق من حبهم لأنفسهم."
        )
    },
    {
        "name": "سمية بنت خياط رضي الله عنها",
        "story": (
            "🌟 *سمية بنت خياط - أول شهيدة في الإسلام*\n\n"
            "سمية رضي الله عنها أم عمار بن ياسر، كانت من أوائل المسلمين، "
            "وكانت مع زوجها ياسر وابنها عمار يُعذَّبون في رمضاء مكة.\n\n"
            "كان رسول الله ﷺ يمر عليهم ويقول: (صبرًا آل ياسر، فإن موعدكم الجنة).\n\n"
            "فجاء أبو جهل وطعن سمية بحربة فاستشهدت، فكانت أول شهيدة في الإسلام.\n\n"
            "💫 *الدرس:* الثبات على الحق مهما كان الثمن."
        )
    },
    {
        "name": "مصعب بن عمير رضي الله عنه",
        "story": (
            "🌟 *مصعب بن عمير - فداء الروح للدين*\n\n"
            "مصعب بن عمير كان من أجمل شباب قريش وأكثرهم ترفًا، "
            "فلما أسلم تركه أهله وجردوه من كل شيء.\n\n"
            "أرسله النبي ﷺ إلى المدينة سفيرًا للإسلام، فأسلم على يديه خلق كثير.\n\n"
            "وفي غزوة أُحُد، حمل لواء المسلمين، فقُطعت يده اليمنى فحمل اللواء بيسراه، "
            "فقُطعت فضمّ اللواء بعضديه حتى استشهد.\n\n"
            "💫 *الدرس:* الإخلاص للمبدأ حتى آخر لحظة."
        )
    },
    {
        "name": "نُسيبة بنت كعب (أم عمارة) رضي الله عنها",
        "story": (
            "🌟 *نُسيبة بنت كعب - سيف الإسلام*\n\n"
            "نُسيبة بنت كعب رضي الله عنها من أبطال الصحابيات، "
            "شهدت بيعة العقبة وغزوة أُحُد وعدة غزوات.\n\n"
            "في أُحُد، حين انكشف المسلمون، وقفت تحمل السيف والترس تدافع عن رسول الله ﷺ، "
            "وأُصيبت بجراح كثيرة، فقال ﷺ: (ما التفتُ يمينًا ولا شمالًا إلا رأيتها تقاتل دوني).\n\n"
            "💫 *الدرس:* المرأة المسلمة درع للإسلام وحامية للحق."
        )
    },
    {
        "name": "بلال بن رباح رضي الله عنه",
        "story": (
            "🌟 *بلال بن رباح - أحد أحد*\n\n"
            "بلال رضي الله عنه كان عبدًا حبشيًا أسلم مبكرًا، "
            "فأخذه سيده أمية بن خلف يُعذّبه في الرمضاء، "
            "يضع على صدره الصخرة الكبيرة ويقول: ارجع عن محمد!\n\n"
            "وكان بلال يقول: (أحد أحد!)\n\n"
            "حتى مرّ أبو بكر الصديق فاشتراه بخمس أواقٍ من الفضة وأعتقه.\n\n"
            "وصار أول مؤذن في الإسلام، قال له النبي ﷺ: (سمعت خشخشة نعليك في الجنة).\n\n"
            "💫 *الدرس:* الثبات في الابتلاء يرفع الدرجات."
        )
    }
]

NIBAAT_INFO = (
    "🌿 *نظام الطيبات - معلومات عامة*\n\n"
    "نظام الطيبات هو منهج غذائي وصحي إسلامي يرتكز على:\n\n"
    "📌 *الأساس القرآني:*\n"
    "• قال الله تعالى: ﴿يَا أَيُّهَا النَّاسُ كُلُوا مِمَّا فِي الْأَرْضِ حَلَالًا طَيِّبًا﴾\n"
    "• قال تعالى: ﴿وَيُحِلُّ لَهُمُ الطَّيِّبَاتِ وَيُحَرِّمُ عَلَيْهِمُ الْخَبَائِثَ﴾\n\n"
    "🥗 *مبادئ الطيبات:*\n"
    "• الأكل الحلال الخالي من الشبهات.\n"
    "• تجنب الإسراف والتبذير في الطعام.\n"
    "• الاعتدال في الكميات.\n"
    "• الأكل بيد اليمين والتسمية قبله.\n"
    "• الاهتمام بالغذاء الصحي المتوازن.\n\n"
    "🍯 *من الطيبات المذكورة في القرآن والسنة:*\n"
    "• العسل: شفاء للناس.\n"
    "• التمر: قوت ودواء.\n"
    "• الزيتون: مبارك ودهنه علاج.\n"
    "• الحليب والعسل في الجنة.\n"
    "• ماء زمزم: شفاء لما شُرب له.\n\n"
    "💡 *للاستفسار عن نظام الطيبات التفصيلي، تواصل مع المختصين الشرعيين.*"
)


async def fetch_reciters(lang: str = "ar") -> list:
    url = f"{MP3QURAN_API}/reciters?language={lang}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("reciters", [])
    return []


async def fetch_rewayat() -> list:
    url = f"{MP3QURAN_API}/rewayat"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("rewayat", [])
    return []


async def fetch_surahs() -> list:
    url = f"{MP3QURAN_API}/suwar?language=ar"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("suwar", [])
    return []


async def fetch_hijri_date() -> dict:
    today = datetime.now()
    url = f"https://api.aladhan.com/v1/gToH/{today.day}-{today.month}-{today.year}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("data", {}).get("hijri", {})
    return {}


async def fetch_hadith() -> str:
    hadiths = [
        "قال رسول الله ﷺ: (إنما الأعمال بالنيات، وإنما لكل امرئ ما نوى) رواه البخاري ومسلم.",
        "قال رسول الله ﷺ: (من كان يؤمن بالله واليوم الآخر فليقل خيرًا أو ليصمت) رواه البخاري.",
        "قال رسول الله ﷺ: (لا يؤمن أحدكم حتى يحب لأخيه ما يحب لنفسه) رواه البخاري ومسلم.",
        "قال رسول الله ﷺ: (الطهور شطر الإيمان) رواه مسلم.",
        "قال رسول الله ﷺ: (خيركم من تعلم القرآن وعلّمه) رواه البخاري.",
        "قال رسول الله ﷺ: (ما نقصت صدقة من مال) رواه مسلم.",
        "قال رسول الله ﷺ: (الدنيا سجن المؤمن وجنة الكافر) رواه مسلم.",
        "قال رسول الله ﷺ: (من سلك طريقًا يلتمس فيه علمًا سهّل الله له طريقًا إلى الجنة) رواه مسلم.",
        "قال رسول الله ﷺ: (البر حسن الخلق) رواه مسلم.",
        "قال رسول الله ﷺ: (خير الناس أنفعهم للناس) صحيح الجامع.",
        "قال رسول الله ﷺ: (اتق الله حيثما كنت، وأتبع السيئة الحسنة تمحها، وخالق الناس بخلق حسن) رواه الترمذي.",
        "قال رسول الله ﷺ: (كل سلامى من الناس عليه صدقة كل يوم تطلع فيه الشمس) رواه البخاري.",
        "قال رسول الله ﷺ: (إن الله جميل يحب الجمال) رواه مسلم.",
        "قال رسول الله ﷺ: (من لا يرحم لا يُرحم) رواه البخاري.",
        "قال رسول الله ﷺ: (المسلم من سلم المسلمون من لسانه ويده) رواه البخاري.",
    ]
    day_of_year = datetime.now().timetuple().tm_yday
    return hadiths[day_of_year % len(hadiths)]


BATTLES = {
    1: {"name": "سرية عبيدة بن الحارث", "year": "1 هـ", "detail": "أول سرية في الإسلام."},
    2: {"name": "غزوة بدر الكبرى", "year": "2 هـ", "detail": "أعظم غزوة، فرّق الله فيها بين الحق والباطل. نصر الله فيها المسلمين على قريش."},
    3: {"name": "غزوة أُحُد", "year": "3 هـ", "detail": "ابتلاء للمسلمين، استشهد فيها حمزة سيد الشهداء."},
    4: {"name": "غزوة بدر الآخرة", "year": "4 هـ", "detail": "غزوة موعد أبو سفيان."},
    5: {"name": "غزوة الخندق (الأحزاب)", "year": "5 هـ", "detail": "حصار المدينة من الأحزاب وانتصار الله للمسلمين بالريح."},
    6: {"name": "صلح الحديبية", "year": "6 هـ", "detail": "فتح مبين، نشر الإسلام في أرجاء الجزيرة."},
    7: {"name": "غزوة خيبر", "year": "7 هـ", "detail": "فتح حصون خيبر وإجلاء اليهود."},
    8: {"name": "فتح مكة", "year": "8 هـ", "detail": "أعظم فتح، دخل رسول الله ﷺ مكة فاتحًا ومنّ على أهلها."},
    9: {"name": "غزوة تبوك", "year": "9 هـ", "detail": "آخر غزوات النبي ﷺ، تحرك نحو الروم."},
    10: {"name": "حجة الوداع", "year": "10 هـ", "detail": "آخر حج للنبي ﷺ، خطب فيها خطبة الوداع الخالدة."},
    11: {"name": "حروب الردة", "year": "11 هـ", "detail": "في عهد أبي بكر الصديق رضي الله عنه."},
    12: {"name": "فتوحات العراق والشام", "year": "12-13 هـ", "detail": "فتح العراق والشام على يد خالد بن الوليد وأبي عبيدة."},
    15: {"name": "معركة القادسية", "year": "15 هـ", "detail": "فتح الله فيها فارس للمسلمين."},
    20: {"name": "فتح مصر", "year": "20 هـ", "detail": "فتح عمرو بن العاص مصر للإسلام."},
}


def get_battle_of_day(hijri_day: int, hijri_month: int) -> str:
    key = hijri_month
    if key in BATTLES:
        b = BATTLES[key]
        return (
            f"⚔️ *غزوة في هذا الشهر الهجري*\n\n"
            f"*{b['name']}* ({b['year']})\n"
            f"{b['detail']}"
        )
    return "⚔️ لا توجد غزوة مسجلة لهذا الشهر في قاعدة البيانات."


AZKAR_SABAH = [
    {"text": "أَعُوذُ بِاللهِ مِنَ الشَّيْطَانِ الرَّجِيمِ\n﴿اللَّهُ لَا إِلَٰهَ إِلَّا هُوَ الْحَيُّ الْقَيُّومُ...﴾ آية الكرسي", "count": "1 مرة"},
    {"text": "بِسْمِ اللهِ الرَّحْمَنِ الرَّحِيمِ ﴿قُلْ هُوَ اللهُ أَحَدٌ...﴾ سورة الإخلاص", "count": "3 مرات"},
    {"text": "بِسْمِ اللهِ الرَّحْمَنِ الرَّحِيمِ ﴿قُلْ أَعُوذُ بِرَبِّ الْفَلَقِ...﴾ سورة الفلق", "count": "3 مرات"},
    {"text": "بِسْمِ اللهِ الرَّحْمَنِ الرَّحِيمِ ﴿قُلْ أَعُوذُ بِرَبِّ النَّاسِ...﴾ سورة الناس", "count": "3 مرات"},
    {"text": "أَصْبَحْنَا وَأَصْبَحَ الْمُلْكُ للهِ، وَالْحَمْدُ للهِ، لَا إِلَٰهَ إِلَّا اللهُ وَحْدَهُ لَا شَرِيكَ لَهُ، لَهُ الْمُلْكُ وَلَهُ الْحَمْدُ وَهُوَ عَلَى كُلِّ شَيْءٍ قَدِيرٌ.", "count": "1 مرة"},
    {"text": "اللَّهُمَّ بِكَ أَصْبَحْنَا، وَبِكَ أَمْسَيْنَا، وَبِكَ نَحْيَا، وَبِكَ نَمُوتُ وَإِلَيْكَ النُّشُورُ.", "count": "1 مرة"},
    {"text": "اللَّهُمَّ أَنْتَ رَبِّي لَا إِلَٰهَ إِلَّا أَنْتَ، خَلَقْتَنِي وَأَنَا عَبْدُكَ، وَأَنَا عَلَى عَهْدِكَ وَوَعْدِكَ مَا اسْتَطَعْتُ، أَعُوذُ بِكَ مِنْ شَرِّ مَا صَنَعْتُ، أَبُوءُ لَكَ بِنِعْمَتِكَ عَلَيَّ، وَأَبُوءُ بِذَنْبِي فَاغْفِرْ لِي فَإِنَّهُ لَا يَغْفِرُ الذُّنُوبَ إِلَّا أَنْتَ. (سيد الاستغفار)", "count": "1 مرة"},
    {"text": "سُبْحَانَ اللهِ وَبِحَمْدِهِ", "count": "100 مرة"},
    {"text": "لَا إِلَٰهَ إِلَّا اللهُ وَحْدَهُ لَا شَرِيكَ لَهُ، لَهُ الْمُلْكُ وَلَهُ الْحَمْدُ وَهُوَ عَلَى كُلِّ شَيْءٍ قَدِيرٌ.", "count": "10 مرات"},
    {"text": "اللَّهُمَّ عَافِنِي فِي بَدَنِي، اللَّهُمَّ عَافِنِي فِي سَمْعِي، اللَّهُمَّ عَافِنِي فِي بَصَرِي، لَا إِلَٰهَ إِلَّا أَنْتَ.", "count": "3 مرات"},
    {"text": "حَسْبِيَ اللهُ لَا إِلَٰهَ إِلَّا هُوَ عَلَيْهِ تَوَكَّلْتُ وَهُوَ رَبُّ الْعَرْشِ الْعَظِيمِ.", "count": "7 مرات"},
    {"text": "بِسْمِ اللهِ الَّذِي لَا يَضُرُّ مَعَ اسْمِهِ شَيْءٌ فِي الْأَرْضِ وَلَا فِي السَّمَاءِ وَهُوَ السَّمِيعُ الْعَلِيمُ.", "count": "3 مرات"},
    {"text": "رَضِيتُ بِاللهِ رَبًّا، وَبِالْإِسْلَامِ دِينًا، وَبِمُحَمَّدٍ ﷺ نَبِيًّا.", "count": "3 مرات"},
]

AZKAR_MASAA = [
    {"text": "أَعُوذُ بِاللهِ مِنَ الشَّيْطَانِ الرَّجِيمِ\n﴿اللَّهُ لَا إِلَٰهَ إِلَّا هُوَ الْحَيُّ الْقَيُّومُ...﴾ آية الكرسي", "count": "1 مرة"},
    {"text": "بِسْمِ اللهِ الرَّحْمَنِ الرَّحِيمِ ﴿قُلْ هُوَ اللهُ أَحَدٌ...﴾ سورة الإخلاص", "count": "3 مرات"},
    {"text": "بِسْمِ اللهِ الرَّحْمَنِ الرَّحِيمِ ﴿قُلْ أَعُوذُ بِرَبِّ الْفَلَقِ...﴾ سورة الفلق", "count": "3 مرات"},
    {"text": "بِسْمِ اللهِ الرَّحْمَنِ الرَّحِيمِ ﴿قُلْ أَعُوذُ بِرَبِّ النَّاسِ...﴾ سورة الناس", "count": "3 مرات"},
    {"text": "أَمْسَيْنَا وَأَمْسَى الْمُلْكُ للهِ، وَالْحَمْدُ للهِ، لَا إِلَٰهَ إِلَّا اللهُ وَحْدَهُ لَا شَرِيكَ لَهُ، لَهُ الْمُلْكُ وَلَهُ الْحَمْدُ وَهُوَ عَلَى كُلِّ شَيْءٍ قَدِيرٌ.", "count": "1 مرة"},
    {"text": "اللَّهُمَّ بِكَ أَمْسَيْنَا، وَبِكَ أَصْبَحْنَا، وَبِكَ نَحْيَا، وَبِكَ نَمُوتُ وَإِلَيْكَ الْمَصِيرُ.", "count": "1 مرة"},
    {"text": "اللَّهُمَّ أَنْتَ رَبِّي لَا إِلَٰهَ إِلَّا أَنْتَ، خَلَقْتَنِي وَأَنَا عَبْدُكَ، وَأَنَا عَلَى عَهْدِكَ وَوَعْدِكَ مَا اسْتَطَعْتُ، أَعُوذُ بِكَ مِنْ شَرِّ مَا صَنَعْتُ، أَبُوءُ لَكَ بِنِعْمَتِكَ عَلَيَّ، وَأَبُوءُ بِذَنْبِي فَاغْفِرْ لِي فَإِنَّهُ لَا يَغْفِرُ الذُّنُوبَ إِلَّا أَنْتَ. (سيد الاستغفار)", "count": "1 مرة"},
    {"text": "اللَّهُمَّ إِنِّي أَمْسَيْتُ أُشْهِدُكَ، وَأُشْهِدُ حَمَلَةَ عَرْشِكَ، وَمَلَائِكَتَكَ، وَجَمِيعَ خَلْقِكَ، أَنَّكَ أَنْتَ اللهُ لَا إِلَٰهَ إِلَّا أَنْتَ وَحْدَكَ لَا شَرِيكَ لَكَ، وَأَنَّ مُحَمَّدًا عَبْدُكَ وَرَسُولُكَ.", "count": "4 مرات"},
    {"text": "سُبْحَانَ اللهِ وَبِحَمْدِهِ", "count": "100 مرة"},
    {"text": "اللَّهُمَّ عَافِنِي فِي بَدَنِي، اللَّهُمَّ عَافِنِي فِي سَمْعِي، اللَّهُمَّ عَافِنِي فِي بَصَرِي، لَا إِلَٰهَ إِلَّا أَنْتَ.", "count": "3 مرات"},
    {"text": "اللَّهُمَّ إِنِّي أَعُوذُ بِكَ مِنَ الْكُفْرِ وَالْفَقْرِ، وَأَعُوذُ بِكَ مِنْ عَذَابِ الْقَبْرِ، لَا إِلَٰهَ إِلَّا أَنْتَ.", "count": "3 مرات"},
    {"text": "بِسْمِ اللهِ الَّذِي لَا يَضُرُّ مَعَ اسْمِهِ شَيْءٌ فِي الْأَرْضِ وَلَا فِي السَّمَاءِ وَهُوَ السَّمِيعُ الْعَلِيمُ.", "count": "3 مرات"},
    {"text": "اللَّهُمَّ إِنِّي أَسْأَلُكَ الْعَفْوَ وَالْعَافِيَةَ فِي الدُّنْيَا وَالْآخِرَةِ.", "count": "3 مرات"},
]


def main_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("📖 القرآن الكريم", callback_data="quran_menu")],
        [InlineKeyboardButton("📅 التقويم الهجري", callback_data="hijri_calendar"),
         InlineKeyboardButton("📿 حديث اليوم", callback_data="daily_hadith")],
        [InlineKeyboardButton("⚔️ غزوة اليوم", callback_data="battle_today"),
         InlineKeyboardButton("🌟 قصص الصحابة", callback_data="sahaba_stories")],
        [InlineKeyboardButton("🌿 نظام الطيبات", callback_data="nibaat_info"),
         InlineKeyboardButton("🌸 قسم المرأة", callback_data="women_section")],
        [InlineKeyboardButton("🌅 أذكار الصباح", callback_data="azkar_sabah"),
         InlineKeyboardButton("🌆 أذكار المساء", callback_data="azkar_masaa")],
        [InlineKeyboardButton("🔔 اشترك في الإشعارات الصباحية", callback_data="subscribe")],
    ]
    return InlineKeyboardMarkup(keyboard)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    name = user.first_name if user else "أخي الكريم"
    welcome_text = (
        f"السلام عليكم ورحمة الله وبركاته 🌙\n\n"
        f"أهلاً وسهلاً {name}!\n\n"
        f"مرحباً بك في بوت القرآن الكريم 📖\n"
        f"اختر ما تريد من القائمة أدناه:"
    )
    await update.message.reply_text(
        welcome_text,
        reply_markup=main_menu_keyboard(),
        parse_mode="Markdown"
    )


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "🌙 *القائمة الرئيسية*\n\nاختر ما تريد:",
        reply_markup=main_menu_keyboard(),
        parse_mode="Markdown"
    )


async def quran_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("👤 اختر قارئ", callback_data="reciters_page_0")],
        [InlineKeyboardButton("🔙 الرئيسية", callback_data="main_menu")],
    ]
    await query.edit_message_text(
        "📖 *القرآن الكريم*\n\nاختر كيف تريد الاستماع:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def show_reciters_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("جاري تحميل القراء...")
    page = int(query.data.split("_")[-1])
    per_page = 8

    reciters = await fetch_reciters()
    if not reciters:
        await query.edit_message_text("⚠️ تعذّر تحميل قائمة القراء، حاول لاحقًا.")
        return

    start_idx = page * per_page
    end_idx = start_idx + per_page
    page_reciters = reciters[start_idx:end_idx]

    keyboard = []
    for r in page_reciters:
        keyboard.append([InlineKeyboardButton(
            f"🎙️ {r['name']}",
            callback_data=f"reciter_{r['id']}"
        )])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"reciters_page_{page-1}"))
    if end_idx < len(reciters):
        nav.append(InlineKeyboardButton("التالي ➡️", callback_data=f"reciters_page_{page+1}"))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🔙 القائمة الرئيسية", callback_data="main_menu")])

    total_pages = (len(reciters) + per_page - 1) // per_page
    await query.edit_message_text(
        f"🎙️ *اختر القارئ* (صفحة {page+1}/{total_pages})\n\n"
        f"إجمالي القراء: {len(reciters)} قارئ",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def show_reciter_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    reciter_id = int(query.data.split("_")[1])

    reciters = await fetch_reciters()
    reciter = next((r for r in reciters if r["id"] == reciter_id), None)

    if not reciter:
        await query.edit_message_text("⚠️ لم يُعثر على بيانات القارئ.")
        return

    rewayat = reciter.get("moshaf", [])
    rewaya_text = "\n".join([f"  • {m.get('name', '')} ({m.get('rewaya', '')})" for m in rewayat]) or "غير محدد"

    info_text = (
        f"🎙️ *{reciter['name']}*\n\n"
        f"🌍 الجنسية: {reciter.get('country', 'غير محدد')}\n"
        f"📚 الروايات المتاحة:\n{rewaya_text}\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"{DUA_ANDALUSIYA}"
    )

    keyboard = []
    for m in rewayat[:5]:
        keyboard.append([InlineKeyboardButton(
            f"📖 {m.get('name', 'غير محدد')}",
            callback_data=f"moshaf_{reciter_id}_{m['id']}"
        )])
    keyboard.append([InlineKeyboardButton("🔙 قائمة القراء", callback_data="reciters_page_0")])

    await query.edit_message_text(
        info_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def show_surah_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("جاري تحميل السور...")

    parts = query.data.split("_")
    reciter_id = int(parts[1])
    moshaf_id = int(parts[2])

    reciters = await fetch_reciters()
    reciter = next((r for r in reciters if r["id"] == reciter_id), None)
    if not reciter:
        await query.edit_message_text("⚠️ خطأ في تحميل البيانات.")
        return

    moshaf = next((m for m in reciter.get("moshaf", []) if m["id"] == moshaf_id), None)
    if not moshaf:
        await query.edit_message_text("⚠️ لم يُعثر على هذه الرواية.")
        return

    server = moshaf.get("server", "")
    surah_list = moshaf.get("surah_list", "")

    surahs = await fetch_surahs()
    available_surahs = [int(x) for x in surah_list.split(",") if x.strip().isdigit()] if surah_list else list(range(1, 115))

    keyboard = []
    row = []
    for i, surah_num in enumerate(available_surahs[:30]):
        surah_info = next((s for s in surahs if s.get("id") == surah_num), None)
        surah_name = surah_info["name"] if surah_info else f"سورة {surah_num}"
        row.append(InlineKeyboardButton(
            surah_name,
            callback_data=f"play_{reciter_id}_{moshaf_id}_{surah_num}"
        ))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("🔙 القارئ", callback_data=f"reciter_{reciter_id}")])

    await query.edit_message_text(
        f"📖 *{reciter['name']}* - *{moshaf['name']}*\n\nاختر السورة:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def play_surah(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("جاري إرسال التلاوة...")

    parts = query.data.split("_")
    reciter_id = int(parts[1])
    moshaf_id = int(parts[2])
    surah_num = int(parts[3])

    reciters = await fetch_reciters()
    reciter = next((r for r in reciters if r["id"] == reciter_id), None)
    if not reciter:
        await query.edit_message_text("⚠️ خطأ في تحميل البيانات.")
        return

    moshaf = next((m for m in reciter.get("moshaf", []) if m["id"] == moshaf_id), None)
    if not moshaf:
        await query.edit_message_text("⚠️ لم يُعثر على هذه الرواية.")
        return

    server = moshaf.get("server", "")
    surah_str = str(surah_num).zfill(3)
    audio_url = f"{server}{surah_str}.mp3"

    surahs = await fetch_surahs()
    surah_info = next((s for s in surahs if s.get("id") == surah_num), None)
    surah_name = surah_info["name"] if surah_info else f"سورة {surah_num}"

    caption = (
        f"📖 *{surah_name}*\n"
        f"🎙️ القارئ: {reciter['name']}\n"
        f"📚 الرواية: {moshaf.get('rewaya', '')}\n\n"
        f"🔗 [استمع مباشرة]({audio_url})\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"{DUA_ANDALUSIYA}"
    )

    keyboard = [[InlineKeyboardButton("🔙 قائمة السور", callback_data=f"moshaf_{reciter_id}_{moshaf_id}")]]

    try:
        await query.edit_message_text(
            caption,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
            disable_web_page_preview=False
        )
        await context.bot.send_audio(
            chat_id=query.message.chat_id,
            audio=audio_url,
            title=f"{surah_name} - {reciter['name']}",
            performer=reciter['name'],
            caption=f"📖 {surah_name} | 🎙️ {reciter['name']}"
        )
    except Exception as e:
        logger.error(f"Error sending audio: {e}")
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=caption,
            parse_mode="Markdown",
            disable_web_page_preview=False
        )


async def hijri_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("جاري تحميل التقويم...")

    hijri = await fetch_hijri_date()

    if hijri:
        day = hijri.get("day", "")
        month_info = hijri.get("month", {})
        month_ar = month_info.get("ar", "") if isinstance(month_info, dict) else str(month_info)
        year = hijri.get("year", "")
        weekday_info = hijri.get("weekday", {})
        weekday_ar = weekday_info.get("ar", "") if isinstance(weekday_info, dict) else ""

        text = (
            f"📅 *التقويم الهجري*\n\n"
            f"🗓️ اليوم: *{weekday_ar}*\n"
            f"📆 التاريخ: *{day} {month_ar} {year} هـ*\n\n"
            f"🌙 *الأشهر الهجرية الحرم:*\n"
            f"المحرم، رجب، ذو القعدة، ذو الحجة\n\n"
            f"🕌 *المناسبات الإسلامية القادمة:*\n"
            f"• 1 المحرم: رأس السنة الهجرية\n"
            f"• 10 المحرم: يوم عاشوراء\n"
            f"• 12 ربيع الأول: المولد النبوي الشريف\n"
            f"• 27 رجب: الإسراء والمعراج\n"
            f"• 15 شعبان: ليلة النصف من شعبان\n"
            f"• رمضان: شهر الصيام والقرآن\n"
            f"• 1 شوال: عيد الفطر المبارك\n"
            f"• 10 ذو الحجة: عيد الأضحى المبارك\n"
        )
    else:
        greg = datetime.now()
        text = (
            f"📅 *التقويم*\n\n"
            f"🗓️ التاريخ الميلادي: {greg.strftime('%d/%m/%Y')}\n"
            f"⚠️ تعذّر تحميل التاريخ الهجري، حاول لاحقًا."
        )

    keyboard = [[InlineKeyboardButton("🔙 الرئيسية", callback_data="main_menu")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def daily_hadith(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    hadith = await fetch_hadith()
    text = f"📿 *حديث اليوم*\n\n{hadith}"

    keyboard = [[InlineKeyboardButton("🔙 الرئيسية", callback_data="main_menu")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def battle_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    hijri = await fetch_hijri_date()
    if hijri:
        month_info = hijri.get("month", {})
        month_num = month_info.get("number", 1) if isinstance(month_info, dict) else 1
        day_num = int(hijri.get("day", 1))
    else:
        month_num = datetime.now().month
        day_num = datetime.now().day

    battle_text = get_battle_of_day(day_num, month_num)

    keyboard = [
        [InlineKeyboardButton("🌟 قصص الصحابة", callback_data="sahaba_stories")],
        [InlineKeyboardButton("🔙 الرئيسية", callback_data="main_menu")]
    ]
    await query.edit_message_text(battle_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def sahaba_stories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    keyboard = []
    for i, story in enumerate(SAHABA_STORIES):
        keyboard.append([InlineKeyboardButton(
            f"🌟 {story['name']}",
            callback_data=f"sahaba_{i}"
        )])
    keyboard.append([InlineKeyboardButton("🔙 الرئيسية", callback_data="main_menu")])

    await query.edit_message_text(
        "🌟 *قصص فداء الصحابة والصحابيات*\n\nاختر القصة التي تريد قراءتها:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def show_sahaba_story(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    idx = int(query.data.split("_")[1])
    story = SAHABA_STORIES[idx]

    keyboard = [
        [InlineKeyboardButton("🔙 قائمة القصص", callback_data="sahaba_stories")],
        [InlineKeyboardButton("🏠 الرئيسية", callback_data="main_menu")]
    ]
    await query.edit_message_text(
        story["story"],
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def nibaat_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    keyboard = [[InlineKeyboardButton("🔙 الرئيسية", callback_data="main_menu")]]
    await query.edit_message_text(NIBAAT_INFO, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def women_section(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    keyboard = []
    for key, topic in WOMEN_TOPICS.items():
        keyboard.append([InlineKeyboardButton(topic["title"], callback_data=f"women_{key}")])
    keyboard.append([InlineKeyboardButton("🔙 الرئيسية", callback_data="main_menu")])

    await query.edit_message_text(
        "🌸 *قسم المرأة المسلمة*\n\n"
        "مرحبًا بك في قسم العلوم الشرعية الخاصة بالمرأة المسلمة.\n"
        "اختري الموضوع الذي تريدين:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def show_women_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    topic_key = "_".join(query.data.split("_")[1:])
    topic = WOMEN_TOPICS.get(topic_key)

    if not topic:
        await query.edit_message_text("⚠️ لم يُعثر على هذا الموضوع.")
        return

    keyboard = [
        [InlineKeyboardButton("🔙 قسم المرأة", callback_data="women_section")],
        [InlineKeyboardButton("🏠 الرئيسية", callback_data="main_menu")]
    ]
    await query.edit_message_text(
        topic["content"],
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


def format_azkar_message(azkar_list: list, title: str, emoji: str) -> str:
    lines = [f"{emoji} *{title}*\n", "━━━━━━━━━━━━━━\n"]
    for i, z in enumerate(azkar_list, 1):
        lines.append(f"*{i}.* {z['text']}\n🔁 _{z['count']}_\n")
    lines.append("━━━━━━━━━━━━━━")
    lines.append("🤲 _اللهم تقبّل منا_")
    return "\n".join(lines)


async def show_azkar_sabah(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("🌅 أذكار الصباح...")
    keyboard = [[InlineKeyboardButton("🔙 الرئيسية", callback_data="main_menu")]]
    msg = format_azkar_message(AZKAR_SABAH, "أذكار الصباح", "🌅")
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_azkar_masaa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("🌆 أذكار المساء...")
    keyboard = [[InlineKeyboardButton("🔙 الرئيسية", callback_data="main_menu")]]
    msg = format_azkar_message(AZKAR_MASAA, "أذكار المساء", "🌆")
    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def send_azkar_broadcast(app: Application, azkar_type: str):
    subs = load_subscribers()
    if not subs:
        return
    if azkar_type == "sabah":
        msg = format_azkar_message(AZKAR_SABAH, "أذكار الصباح", "🌅")
        header = "🌅 *حان وقت أذكار الصباح*\n\n"
    else:
        msg = format_azkar_message(AZKAR_MASAA, "أذكار المساء", "🌆")
        header = "🌆 *حان وقت أذكار المساء*\n\n"

    full_msg = header + msg
    success = 0
    for chat_id in list(subs):
        try:
            await app.bot.send_message(chat_id=chat_id, text=full_msg, parse_mode="Markdown")
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.warning(f"فشل إرسال الأذكار لـ {chat_id}: {e}")
    logger.info(f"📿 أُرسلت أذكار {'الصباح' if azkar_type == 'sabah' else 'المساء'} لـ {success} مشترك")


async def subscribe_notifications(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    subs = load_subscribers()

    if chat_id in subs:
        keyboard = [
            [InlineKeyboardButton("🔕 إلغاء الاشتراك", callback_data="unsubscribe")],
            [InlineKeyboardButton("🔙 الرئيسية", callback_data="main_menu")],
        ]
        await query.edit_message_text(
            "✅ *أنت مشترك بالفعل في الإشعارات اليومية!*\n\n"
            "📬 تصلك كل صباح:\n"
            "• آية قرآنية مع تفسيرها\n"
            "• حديث نبوي شريف\n\n"
            "هل تريد إلغاء الاشتراك؟",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    else:
        subs.add(chat_id)
        save_subscribers(subs)
        keyboard = [[InlineKeyboardButton("🔙 الرئيسية", callback_data="main_menu")]]
        await query.edit_message_text(
            "🎉 *تم اشتراكك بنجاح في الإشعارات اليومية!*\n\n"
            "📬 ستصلك كل يوم في الصباح:\n"
            "• 🌿 آية قرآنية مع تفسيرها\n"
            "• 📿 حديث نبوي شريف\n\n"
            f"👥 إجمالي المشتركين: *{len(subs)}*\n\n"
            "بارك الله فيك وجزاك خيراً 🌙",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )


async def unsubscribe_notifications(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    subs = load_subscribers()
    subs.discard(chat_id)
    save_subscribers(subs)
    keyboard = [
        [InlineKeyboardButton("🔔 اشترك مجدداً", callback_data="subscribe")],
        [InlineKeyboardButton("🔙 الرئيسية", callback_data="main_menu")],
    ]
    await query.edit_message_text(
        "🔕 *تم إلغاء اشتراكك في الإشعارات اليومية.*\n\n"
        "يمكنك الاشتراك مجدداً في أي وقت.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def send_daily_notifications(app: Application):
    subs = load_subscribers()
    if not subs:
        logger.info("لا يوجد مشتركون لإرسال الإشعارات.")
        return

    day_index = datetime.now().timetuple().tm_yday
    ayah = DAILY_AYAT[day_index % len(DAILY_AYAT)]
    hadith = await fetch_hadith()
    hijri = await fetch_hijri_date()

    hijri_text = ""
    if hijri:
        month_info = hijri.get("month", {})
        month_ar = month_info.get("ar", "") if isinstance(month_info, dict) else ""
        day_h = hijri.get("day", "")
        year_h = hijri.get("year", "")
        hijri_text = f"📅 {day_h} {month_ar} {year_h} هـ\n"

    message = (
        f"🌅 *صباح النور والإيمان*\n"
        f"{hijri_text}"
        f"━━━━━━━━━━━━━━\n\n"
        f"🌿 *آية اليوم*\n"
        f"سورة {ayah['surah']} — آية {ayah['ayah']}\n\n"
        f"{ayah['text']}\n\n"
        f"💡 *من التفسير:* {ayah['tafseer']}\n\n"
        f"━━━━━━━━━━━━━━\n\n"
        f"📿 *حديث اليوم*\n"
        f"{hadith}\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🤲 {DUA_ANDALUSIYA}"
    )

    success = 0
    failed = 0
    for chat_id in list(subs):
        try:
            await app.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode="Markdown"
            )
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.warning(f"فشل إرسال إشعار لـ {chat_id}: {e}")
            failed += 1

    logger.info(f"📬 الإشعارات: {success} ناجح، {failed} فاشل من {len(subs)} مشترك")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if any(word in text for word in ["السلام", "مرحب", "هلا", "أهلا"]):
        await update.message.reply_text(
            "وعليكم السلام ورحمة الله وبركاته 🌙\n\nاضغط /start للبدء.",
        )
    else:
        await update.message.reply_text(
            "📱 اضغط /start لفتح القائمة الرئيسية.",
        )


def register_handlers(app: Application):
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", start))

    app.add_handler(CallbackQueryHandler(show_main_menu, pattern="^main_menu$"))
    app.add_handler(CallbackQueryHandler(quran_menu, pattern="^quran_menu$"))
    app.add_handler(CallbackQueryHandler(show_reciters_page, pattern="^reciters_page_"))
    app.add_handler(CallbackQueryHandler(show_reciter_info, pattern="^reciter_\\d+$"))
    app.add_handler(CallbackQueryHandler(show_surah_list, pattern="^moshaf_"))
    app.add_handler(CallbackQueryHandler(play_surah, pattern="^play_"))
    app.add_handler(CallbackQueryHandler(hijri_calendar, pattern="^hijri_calendar$"))
    app.add_handler(CallbackQueryHandler(daily_hadith, pattern="^daily_hadith$"))
    app.add_handler(CallbackQueryHandler(battle_today, pattern="^battle_today$"))
    app.add_handler(CallbackQueryHandler(sahaba_stories, pattern="^sahaba_stories$"))
    app.add_handler(CallbackQueryHandler(show_sahaba_story, pattern="^sahaba_\\d+$"))
    app.add_handler(CallbackQueryHandler(nibaat_info, pattern="^nibaat_info$"))
    app.add_handler(CallbackQueryHandler(women_section, pattern="^women_section$"))
    app.add_handler(CallbackQueryHandler(show_women_topic, pattern="^women_"))
    app.add_handler(CallbackQueryHandler(subscribe_notifications, pattern="^subscribe$"))
    app.add_handler(CallbackQueryHandler(unsubscribe_notifications, pattern="^unsubscribe$"))
    app.add_handler(CallbackQueryHandler(show_azkar_sabah, pattern="^azkar_sabah$"))
    app.add_handler(CallbackQueryHandler(show_azkar_masaa, pattern="^azkar_masaa$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))


async def post_init(app: Application):
    scheduler = AsyncIOScheduler(timezone="Asia/Riyadh")

    scheduler.add_job(
        send_daily_notifications,
        trigger=CronTrigger(hour=6, minute=0),
        args=[app],
        id="daily_morning",
        name="الإشعار الصباحي (آية + حديث)",
        replace_existing=True,
    )
    scheduler.add_job(
        send_azkar_broadcast,
        trigger=CronTrigger(hour=5, minute=30),
        args=[app, "sabah"],
        id="azkar_sabah",
        name="أذكار الصباح",
        replace_existing=True,
    )
    scheduler.add_job(
        send_azkar_broadcast,
        trigger=CronTrigger(hour=16, minute=0),
        args=[app, "masaa"],
        id="azkar_masaa",
        name="أذكار المساء",
        replace_existing=True,
    )

    scheduler.start()
    app.bot_data["scheduler"] = scheduler
    logger.info("✅ الجدول يعمل: أذكار الصباح 5:30 | إشعار صباحي 6:00 | أذكار المساء 4:00 عصراً (السعودية)")


async def post_shutdown(app: Application):
    scheduler = app.bot_data.get("scheduler")
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("🛑 جدول الإشعارات أُوقف.")


def main():
    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN غير موجود!")
        sys.exit(1)

    retry_delay = 5
    max_delay = 60
    attempt = 0

    while True:
        try:
            attempt += 1
            logger.info(f"🤖 تشغيل البوت... (المحاولة {attempt})")

            app = (
                Application.builder()
                .token(TOKEN)
                .post_init(post_init)
                .post_shutdown(post_shutdown)
                .build()
            )
            register_handlers(app)

            app.run_polling(
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES,
                poll_interval=1.0,
                timeout=30,
            )

            logger.info("البوت توقف بشكل طبيعي.")
            break

        except RetryAfter as e:
            wait = e.retry_after + 1
            logger.warning(f"⏳ Telegram طلب الانتظار {wait} ثانية...")
            time.sleep(wait)

        except (NetworkError, TimedOut) as e:
            logger.warning(f"⚠️ خطأ في الشبكة: {e} — إعادة المحاولة خلال {retry_delay}s")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_delay)

        except KeyboardInterrupt:
            logger.info("🛑 إيقاف البوت يدوياً.")
            break

        except Exception as e:
            logger.error(f"❌ خطأ غير متوقع: {e} — إعادة المحاولة خلال {retry_delay}s")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_delay)

        else:
            retry_delay = 5


if __name__ == "__main__":
    main()
