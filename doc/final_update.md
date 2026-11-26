# گزارش آپدیت نهایی Business Discovery API

تاریخ اجرا: ۲۶ نوامبر ۲۰۲۶  
مسیر پروژه: `/opt/business-discovery`

## نتیجه

API به معماری job غیرهمزمان با ذخیره‌سازی پایدار SQLite تغییر کرد و روی VPS راه‌اندازی شد. نتیجهٔ هر وب‌سایت بلافاصله پس از تکمیل در وضعیت job و event log ذخیره می‌شود؛ کار حتی با قطع SSE ادامه دارد. تست زنده با SearXNG و Firecrawl واقعی موفق شد.

دسترسی عمومی امن هنوز فعال نیست؛ در حال حاضر نام DNS قابل استفاده و گواهی معتبر برای این سرور موجود نیست. سرویس فقط روی `127.0.0.1:8091` گوش می‌دهد. طبق محدودیت‌های زیرساخت، Nginx، فایروال و پورت‌های عمومی تغییر نکرده‌اند؛ بنابراین پروژه را هنوز production-ready از نظر دسترسی Chabokan اعلام نمی‌کنیم.

## پیاده‌سازی

- `POST /v1/discover` اکنون job می‌سازد و با HTTP `202` شناسه و لینک‌های status، events و result را فوری برمی‌گرداند.
- `GET /v1/jobs/{job_id}` وضعیت و شمارنده‌ها را برمی‌گرداند.
- `GET /v1/jobs/{job_id}/events` رویدادهای SSE را با ID ترتیبی، replay از `Last-Event-ID` یا `last_event_id` و heartbeat دوره‌ای ارسال می‌کند.
- `GET /v1/jobs/{job_id}/result` در زمان اجرا `409` و پس از پایان نتیجهٔ نهایی را می‌دهد.
- نتایج وب‌سایت‌های موفق در رویداد `website_completed` همراه شماره و source URL منتشر می‌شوند. شماره‌های تکراری در کل job حذف می‌شوند.
- Jobها و رویدادها در SQLite با WAL نگهداری می‌شوند؛ قفل فایل از اجرای worker دوم روی همان DB جلوگیری می‌کند. jobهای ناتمام بعد از restart بازیابی می‌شوند و داده‌ها پس از ۷ روز منقضی می‌شوند.
- cursor صفحه‌بندی برای هر query دقیق SearXNG جداگانه ذخیره می‌شود. ادامهٔ همان query از صفحهٔ بعدی است؛ دامنه‌های پردازش‌شده در دورهای بعد حذف می‌شوند. صفحهٔ خالی از خطای provider تفکیک شده و خطا به‌عنوان نتیجهٔ خالی پنهان نمی‌شود.
- منطق سه دور، هدف `ceil(remaining_successful_websites * 1.6)`، محدودیت crawl و قواعد استخراج/نرمال‌سازی شمارهٔ موبایل حفظ شده‌اند.

## فایل‌های تغییرکرده

- `.gitignore` — فایل‌های runtime دیتابیس در `data/` از Git خارج شدند.
- `README.md` — workflow غیرهمزمان، endpointها، مثال SSE، resume، تنظیمات، deployment و نیازمندی Chabokan مستند شد.
- `app/main.py` — API job/SSE، امنیت Bearer، cursor صفحه‌بندی و lifespan برنامه.
- `app/job_manager.py` — worker پایدار، progress، پردازش همزمان وب‌سایت و انتشار نتیجهٔ incremental.
- `app/job_store.py` — ذخیرهٔ SQLite برای job و eventها، recovery، انقضا و idempotency.
- `tests/test_discovery.py` — تست‌های regression و خطاهای SearXNG/Firecrawl.
- `tests/test_jobs.py` — تست‌های job، ذخیره/recovery، event order/replay، pagination، dedup و قطع مصرف‌کننده.
- `test_service.py` — smoke test زنده برای API job، status/result، اعتبارسنجی شماره‌ها و replay واقعی SSE.
- `deploy/nginx-business-discovery.conf.example` — نمونهٔ reverse proxy HTTPS و تنظیمات SSE؛ عمداً فعال نشده است.
- `/etc/systemd/system/business-discovery.service` — مسیر DB، TTL، umask محدود و مجوز نوشتن دایرکتوری داده اضافه شد.
- `doc/final_update.md` — همین گزارش.
- `doc/final_update_request.md` — نسخهٔ اصلی درخواست کاربر نگهداری شد، چون مسیر گزارش با مسیر فایل دستورالعمل یکی بود.

فایل پشتیبان پیش از deployment در `/var/backups/business-discovery/final-update-20261126074848/` قرار دارد. فایل `.env` تغییر نکرد و مقادیر محرمانه در گزارش یا لاگ چاپ نشده‌اند.

## پوشه‌های به‌روزشده

- `/opt/business-discovery` — README، `.gitignore` و اسکریپت smoke test.
- `/opt/business-discovery/app` — API و اجزای job/persistence.
- `/opt/business-discovery/tests` — تست‌های واحد و regression.
- `/opt/business-discovery/deploy` — نمونهٔ پیکربندی Nginx.
- `/opt/business-discovery/doc` — گزارش نهایی و نسخهٔ نگهداری‌شدهٔ درخواست.
- `/opt/business-discovery/data` — دایرکتوری خصوصی SQLite که systemd به سرویس اجازهٔ نوشتن در آن می‌دهد.
- `/etc/systemd/system` — unit مربوط به همین سرویس.

## Deployment و وضعیت سرویس‌ها

- `business-discovery.service`: فعال، یک worker، bind فقط روی `127.0.0.1:8091`.
- SearXNG: endpoint داخلی `http://127.0.0.1:8889`، readiness درخواست جست‌وجو HTTP 200.
- Firecrawl: endpoint داخلی `http://127.0.0.1:3002`، readiness HTTP 200.
- Nginx: پیکربندی موجود با `nginx -t` معتبر است و فعال ماند؛ هیچ فایل فعال Nginx تغییر نکرد.
- پورت 443 listener ندارد؛ پورت‌های Firecrawl و SearXNG همچنان روی loopback هستند.
- Git branch روی VPS و remote `main/main` هر دو در commit `0a85f06851fe8b20c78012f1b980f57453043a67` بودند؛ تغییرات این گزارش هنوز commit/push نشده‌اند.

نمونهٔ Nginx در `deploy/nginx-business-discovery.conf.example` شامل HTTPS، redirect از HTTP، محدودیت نرخ ایجاد job، محدودیت اتصال SSE، اندازهٔ body و خاموش‌کردن buffering است. به دلیل hostname و certificate ناموجود، امکان فعال‌سازی یا تست SSE از مسیر reverse proxy وجود نداشت.

## تست‌های اجراشده

- `.venv/bin/python -m unittest discover -s tests -v` — موفق؛ ۳۴ تست.
- `.venv/bin/python -m compileall -q app tests test_service.py` — موفق.
- `systemd-analyze verify /etc/systemd/system/business-discovery.service` — موفق؛ فقط هشدارهای قدیمی systemd دربارهٔ unitهای سیستمی نامرتبط گزارش شد.
- `nginx -t` — پیکربندی فعال فعلی موفق. فایل نمونهٔ API فعال نشده است.
- `curl --fail http://127.0.0.1:8091/health` — موفق.
- `curl ... http://127.0.0.1:8889/search?format=json&q=test` — HTTP 200.
- `curl ... http://127.0.0.1:3002/v0/health/readiness` — HTTP 200.
- `.venv/bin/python test_service.py` — موفق با سرویس‌های واقعی: job `6f9543f6bb184f22ab6ca6807ba6a1c7` در دور اول تمام شد؛ ۲ وب‌سایت پیدا و پردازش شدند، ۱ سایت موفق بود و ۲ شمارهٔ موبایل یکتا با source URL برگشتند. SSE replay شامل ۱۴ رویداد مرتب با IDهای ۱ تا ۱۴ بود.

تست زنده به `http://127.0.0.1:8091` وصل شد. تست از بیرون VPS یا از طریق Nginx انجام نشده، چون proxy عمومی هنوز پیکربندی نشده است.

## اطلاعات لازم برای HTTPS عمومی و Chabokan

برای ادامهٔ مرحلهٔ انتشار امن، hostnameای که مالک یا مدیر DNS آن هستید ارسال کنید. رکورد عمومی `A` باید به `195.248.240.152` اشاره کند؛ اگر `AAAA` تنظیم می‌شود، IPv6 سرور هم باید واقعاً در دسترس باشد. برای دریافت گواهی ACME از روش HTTP-01، پورت‌های 80 و 443 باید از اینترنت به همین VPS برسند. پس از داشتن hostname و گواهی trusted می‌توان template را با همان hostname تنظیم، در مسیر جداگانه نصب و قبل از reload اعتبارسنجی کرد، سپس health و SSE را از مسیر HTTPS آزمایش کرد.

پس از تکمیل آن مرحله، Chabokan از `https://<hostname>/v1/discover` با Bearer token فعلی درخواست می‌زند و با `job_id`، endpointهای status/events/result را دنبال می‌کند. URL عمومی نهایی هنوز قابل اعلام نیست. هیچ تغییری در پروژهٔ Chabokan انجام نشده است.
