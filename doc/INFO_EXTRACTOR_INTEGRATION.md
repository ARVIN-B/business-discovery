# اتصال Info Extractor به Business Discovery

این سند قرارداد فعلی API را برای مخزنی که Business Discovery را فراخوانی می‌کند ثبت می‌کند. Info Extractor می‌تواند روی سرور دیگری اجرا شود؛ در آن حالت `127.0.0.1` مقصد قابل‌استفاده نیست و باید آدرس HTTPS یا VPN قابل‌دسترسی از سرور Info تنظیم شود.

## تنظیم مقصد و کلید

سرویس Business Discovery به‌صورت محلی روی پورت `8091` اجرا می‌شود. یک reverse proxy یا تونل VPN باید آدرس بیرونی را به `127.0.0.1:8091` روی همین سرور هدایت کند. آدرس بیرونی باید مسیرهای `/v1/...` و پاسخ streaming از نوع `text/event-stream` را عبور دهد.

در تنظیمات محیط Info Extractor:

```dotenv
BUSINESS_DISCOVERY_ENABLED=True
BUSINESS_DISCOVERY_BASE_URL=https://business-discovery.example.com
BUSINESS_DISCOVERY_API_KEY=<همان API_KEY تنظیم‌شده در سرور Business Discovery>
BUSINESS_DISCOVERY_CONNECT_TIMEOUT=10
BUSINESS_DISCOVERY_READ_TIMEOUT=45
BUSINESS_DISCOVERY_MAX_RECONNECTS=5
```

`BUSINESS_DISCOVERY_BASE_URL` فقط origin است؛ `/v1` را به انتهای آن اضافه نکنید. مقدار `API_KEY` در Business Discovery باید تصادفی و حداقل ۳۲ کاراکتر باشد. کلید را در Git قرار ندهید. از همان URL و کلید در فرایند وب و تمام Celery workerهای Info Extractor استفاده کنید.

## ایجاد درخواست

Info Extractor این درخواست را می‌فرستد:

```http
POST /v1/discover
Authorization: Bearer <API_KEY>
Content-Type: application/json
Idempotency-Key: <کلید ثابت برای همان Job محلی>
```

```json
{"keyword":"موبایل","province":"اصفهان","count":null}
```

برای جست‌وجوی کشوری مقدار `province` برابر `ایران` است. `count: null` یعنی Business Discovery نباید تعداد شماره‌های هدف Info Extractor را به‌عنوان سقف URL یا Crawl در نظر بگیرد؛ Info مالک هدف یکتای مشترک بین منابع است و فقط هنگام لغو باید stop بفرستد. در retryهای ایجاد Job، همان `Idempotency-Key` را نگه دارید.

پاسخ ایجاد Job شامل `job_id` و آدرس‌های `events_url`، `status_url`، `result_url` و `stop_url` است. پاسخ‌ها باید از همان origin تنظیم‌شده باشند. تمام endpointهای `/v1` نیاز به Bearer token دارند.

## جریان رویداد و لغو

رویدادها را از `events_url` با `Accept: text/event-stream` بخوانید. پس از reconnect، `Last-Event-ID` آخرین رویدادِ پذیرفته‌شده را بفرستید. `url_completed` برای هر URL می‌آید، از جمله URL بدون شماره یا Crawl ناموفق. شماره‌های جدید را از `data.results` بخوانید، در دیتابیس Info ذخیره کنید و تعداد یکتای کلی را دوباره محاسبه کنید.

در نسخهٔ جدید Business Discovery نیازی به ارسال تصمیم برای ادامه نیست؛ بعد از هر `url_completed` خودش URL بعدی را پردازش می‌کند. اگر کاربر Job را لغو کرد، این درخواست را بفرستید:

```http
POST /v1/jobs/<job_id>/stop
Authorization: Bearer <API_KEY>
```

مسیر Boolean برای سازگاری موقت با workerهای قدیمی نگه داشته شده است. `true` no-op است و `false` درخواست توقف می‌دهد:

```http
POST /v1/jobs/<job_id>/decisions/<decision_id>
Authorization: Bearer <API_KEY>
Content-Type: application/json
```

بدنه باید دقیقاً `true` یا `false` باشد؛ شیئی مثل `{"continue":true}` معتبر نیست. این مسیر deprecated است؛ در نسخه‌های جدید فقط هنگام لغو صریح، stop endpoint را صدا بزنید. همان decision ID و Boolean را در retry درخواست قدیمی تکرار کنید؛ cursor SSE را پس از ذخیرهٔ رویداد در Info جلو ببرید.

Info Extractor می‌تواند همچنان `keyword`، `province` و `count: null` بفرستد. برای نسخهٔ جدید توصیه می‌شود Boolean تصمیم را حذف و لغو را به stop endpoint وصل کند. چون `true` دیگر لازم نیست، انتظار برای پاسخ تصمیم نباید بین رویدادهای SSE قرار بگیرد.

## بررسی نتیجهٔ بدون URL

`request_count` در صفحهٔ Info تعداد فراخوانی‌های HTTP از Info به Business Discovery است، نه تعداد queryها یا صفحات SearXNG. برای بررسی علت، از `status_url` یا `result_url` با همان Bearer token استفاده کنید و این فیلدها را بخوانید:

- `reason` و `search_exhausted`
- `search_diagnostics.query_count`, `queries_attempted`, `queries_consumed`, `queries_exhausted`
- `search_diagnostics.pages`, `raw_results`, `valid_urls`, `new_urls`, `duplicate_urls`, `rejected_urls`
- `query_diagnostics`: `status`, `next_page`, `last_error`, `retry_at` و شمارنده‌های هر query

پروژهٔ Business Discovery عبارت‌های فارسی را هم با محدودیت `site:.ir` و هم با fallback محلی می‌فرستد. دامنهٔ `.ir` پذیرفته می‌شود؛ دامنهٔ دیگر فقط وقتی پذیرفته می‌شود که عنوان یا snippet نتیجه، محدودهٔ درخواستی را به فارسی ذکر کند. پارامتر SearXNG به نام `language=en-US` فقط ترجیح زبان موتور جست‌وجو است؛ متن query انگلیسی نمی‌شود. پس از تغییر کد Business Discovery، سرویس آن باید از مسیر انتشار همان سرور به‌روزرسانی شود؛ این تغییر به migration دیتابیس Info Extractor نیاز ندارد.
