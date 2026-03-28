# Groovon Artist Pipeline 🎵

> بايبلاين لاستخراج وتصنيف وإثراء بيانات الفنانين من ملفات Excel.

## 📁 هيكل المشروع

```
groovon/new/
├── .env                        ← 🔑 مفاتيح الـ APIs (لا ترفعه على GitHub!)
├── .env.example                ← 🧩 Template جاهز للمتغيرات (للـ web app handoff)
├── process_david_excel.py      ← 🚀 نقطة التشغيل الرئيسية
│
├── pipeline/                   ← ⚙️ المحرك الأساسي
│   ├── config.py               ← تحميل API keys + ثوابت + أنواع الموسيقى
│   ├── fetchers.py             ← كل الـ API calls (Spotify, MusicBrainz, Last.fm, إلخ)
│   ├── ai_engine.py            ← تصنيف وإثراء بالذكاء الاصطناعي (Gemini/OpenRouter)
│   ├── excel_io.py             ← قراءة وكتابة ملفات Excel
│   └── checkpoint.py           ← نظام الحفظ التلقائي (لو البرنامج وقف تكمل من آخر نقطة)
│
├── tools/
│   └── profile_benchmark.py    ← قياس دقة البروفايل مقابل Golden Dataset
│
├── logs/                       ← 📋 سجلات المشروع
├── _archive/                   ← 📦 ملفات قديمة (مش مستخدمة)
└── README.md                   ← 📖 أنت هنا!
```

## 🚀 طريقة التشغيل

```bash
# تشغيل كل الأحداث
python process_david_excel.py

# استئناف من حدث معين (مثلاً 210)
python process_david_excel.py 210

# تشغيل أول 100 حدث بس
python process_david_excel.py 0 100

# أو من الـ engine الجديد (source -> sinks)
python run.py excel --input "input.xlsx" --output "output.xlsx" --json-out "profiles_rich.json"
```

## ⚙️ مراحل البايبلاين

| المرحلة | الوصف |
|---------|-------|
| **Phase 1-2** | AI بيصنّف الأحداث → يطلّع أسماء فنانين + نوع الموسيقى + نوع الحدث |
| **Phase 2.5** | التحقق من Spotify + MusicBrainz |
| **Phase 3** | إثراء بالـ AI (سيرة ذاتية، إيميلات، روابط) |
| **Phase 3.5** | بناء بروفايلات من 13 مصدر بيانات |
| **Phase 4** | التحقق من الروابط + صحة الإيميلات |
| **Phase 5** | كتابة ملف Excel النهائي |

## 🔌 مصادر البيانات (13 مصدر)

Spotify · MusicBrainz · Last.fm · Wikipedia · Wikidata · iTunes · Discogs · YouTube · Genius · Google Knowledge Graph · Setlist.fm · Serper/DuckDuckGo · Linktree

## 🔑 الـ API Keys المطلوبة (في ملف `.env`)

- `GOOGLE_API_KEY` — Google AI Studio (مجاني)
- `OPENROUTER_API_KEY` — OpenRouter (مدفوع، احتياطي)
- `SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET`
- `LASTFM_API_KEY`
- `YOUTUBE_API_KEY`
- `DISCOGS_TOKEN`
- `GENIUS_ACCESS_TOKEN`
- `GOOGLE_KG_API_KEY`
- `SERPER_API_KEY`
- `SETLISTFM_API_KEY`
- `SCRAPINGBEE_API_KEY`
- `SOUNDCLOUD_CLIENT_ID`
- `BANDSINTOWN_APP_ID`
- `SEATGEEK_CLIENT_ID`
- `TICKETMASTER_API_KEY`
- `EMAIL_VERIFIER_PROVIDER` (`zerobounce` أو `neverbounce` أو `abstract`)
- `ZEROBOUNCE_API_KEY` أو `NEVERBOUNCE_API_KEY` أو `ABSTRACT_API_KEY`
- `SUPABASE_URL` / `SUPABASE_KEY` (لو هترفع على Supabase)

## 📏 قياس دقة البروفايل (Golden Benchmark)

```bash
python tools/profile_benchmark.py ^
  --predicted profiles_rich.json ^
  --golden tmp/golden_profiles.json ^
  --report-out tmp/benchmark_report.json ^
  --fail-below 0.80
```

صيغة السجل في ملف `golden_profiles.json`:

```json
[
  {
    "canonical_artist_id": "ar_abc123...",
    "name": "Artist Name",
    "confidence": "HIGH",
    "min_profile_score": 75,
    "genre": "Jazz",
    "locale": { "city": "London", "country": "United Kingdom" },
    "platforms": ["spotify", "youtube", "website"],
    "emails": true,
    "must_pass_high": true
  }
]
```

السكريبت بيطلع:
- `coverage` (نسبة الـ golden records اللي اتلاقالها profile)
- دقة كل حقل لوحده (`confidence`, `genre`, `locale`, `emails`, ...)
- `platform precision/recall/f1`
- `overall_score` موحد تقدر تستخدمه كـ gate في CI
