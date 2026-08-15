# Zalo Bot + Dashboard — Deploy lên Render (free, chạy 24/7)

Bot Zalo + Gemini AI, chạy 24/7 miễn phí trên Render, kèm 1 trang web xem trạng thái
và log real-time.

## Vì sao chọn Render?

So các dịch vụ hosting phổ biến (Railway, Fly.io, Render) tính tới giữa 2026:
- **Railway**: không còn free tier, chỉ có $5 credit dùng thử 30 ngày
- **Fly.io**: không còn free tier, chỉ có 2 tiếng dùng thử
- **Render**: vẫn còn free tier thật — nhưng service sẽ **tự ngủ sau 15 phút không có
  ai truy cập**, và mất 30-50s để "thức dậy" cho request đầu tiên. Ta sẽ khắc phục
  bằng cách dùng 1 dịch vụ ping miễn phí để giữ nó luôn thức (xem Bước 4).

## Bước 1 — Push code lên GitHub

Push toàn bộ thư mục này lên 1 repo GitHub mới (riêng, không chung với edutest-vn
hay bản zalo-bot-python cũ).

## Bước 2 — Tạo Web Service trên Render

1. Vào [render.com](https://render.com), đăng nhập bằng GitHub
2. **New** → **Web Service** → chọn repo vừa push
3. Render sẽ tự đọc file `render.yaml` và điền sẵn cấu hình (Python, free plan,
   build command, start command) — bro chỉ cần bấm tiếp
4. Ở phần Environment Variables, điền:
   - `ZALO_BOT_TOKEN` — token bot Zalo (từ Zalo Bot Creator trong app Zalo)
   - `GEMINI_API_KEY` — API key Gemini (bro đã có từ EduTest)
   - `PUBLIC_URL` — **bắt buộc** để gửi ảnh/voice: là domain Render cấp,
     vd `https://zalo-bot-xxxx.onrender.com` (không có dấu `/` cuối)
5. Bấm **Create Web Service**, đợi build xong (vài phút)
   - Build command tự cài thêm **ffmpeg** (cần để convert voice sang `.aac`)

### BẮT BUỘC: bật loại tin nhắn cho bot (nguyên nhân số 1 gửi ảnh/sticker/voice thất bại)

Vào **Zalo Bot Creator** (OA "Zalo Bot Manager" trong app Zalo) → chọn bot của bro →
kiểm tra cấu hình **loại tin nhắn bot được phép gửi**: bật đủ **Văn bản, Hình ảnh,
Sticker, Thoại (voice)**. Nếu thiếu loại nào, API trả lỗi khi gửi loại đó dù code
đúng 100%.

## Bước 3 — Kiểm tra dashboard

Sau khi deploy xong, Render cho 1 URL dạng `https://zalo-bot-xxxx.onrender.com`.
Mở link đó lên → thấy trang dashboard hiện trạng thái bot + log real-time.

Nếu thấy `bot_running: false` kèm lỗi thiếu biến môi trường, kiểm tra lại bước 2.4.

## Bước 4 — Giữ cho service không bị ngủ (quan trọng!)

Free tier Render tự tắt service sau 15 phút không có request nào tới. Vì bot cần
chạy liên tục 24/7 để nhận tin nhắn Zalo, cần 1 dịch vụ bên ngoài **ping định kỳ**
vào URL để giữ nó luôn thức:

1. Đăng ký free tại [UptimeRobot.com](https://uptimerobot.com) (không cần thẻ)
2. Tạo **New Monitor**:
   - Monitor Type: HTTP(s)
   - URL: `https://<domain-render-cua-bro>/health`
   - Monitoring Interval: **5 phút** (free tier UptimeRobot cho tối thiểu 5 phút,
     đủ để giữ Render thức vì ngưỡng ngủ là 15 phút)
3. Lưu lại — UptimeRobot giờ sẽ tự ping mỗi 5 phút, Render không bao giờ kịp ngủ

## Tính năng media (ảnh / voice / sticker)

Bot tự quyết định gửi media qua Gemini function calling — không cần lệnh:

- **Ảnh**: nhắn kiểu `vẽ cho mình một chú mèo đội nón lá` → Gemini tự tạo ảnh và gửi.
  Vẫn còn lệnh `/anh <mô tả>` để gửi chủ động.
- **Voice**: nhắn kiểu `gửi voice cho mình` → bot soạn nội dung, TTS bằng `edge-tts`
  (giọng `vi-VN-HoaiMyNeural`, đổi qua biến `ZALO_VOICE`) và gửi file `.aac`.
  Chỉ hoạt động trong chat 1-1 (giới hạn của API Zalo), không gửi được vào nhóm.
- **Sticker**: cài thư viện sticker trên dashboard (tab Cài đặt) → Gemini tự gửi
  sticker đúng ngữ cảnh. Muốn thu thập mã sticker, gửi sticker đó cho bot, bot sẽ
  trả về mã ID để dán vào thư viện.

Nếu gửi ảnh/sticker/voice bị lỗi, xem log trên dashboard — 90% là do loại tin nhắn
chưa bật trên Zalo Bot Creator hoặc thiếu `PUBLIC_URL`.

## Sau này thêm tính năng

Sửa logic trong hàm `echo()` và `handle_photo()` trong `main.py`. Muốn thêm thẻ
thống kê mới lên dashboard thì sửa phần HTML trong hàm `dashboard()` và thêm field
vào dict `stats`. Thêm tool mới cho Gemini thì khai báo trong `build_tools()`.

## Lưu ý

- Ngữ cảnh hội thoại (`chat_sessions`) và log hiện lưu trong RAM — mỗi lần Render
  deploy lại (push code mới) sẽ bị reset về 0, đây là bình thường với free tier.
- Ảnh AI và voice TTS cũng lưu tạm trong RAM (tối đa 50 file mỗi loại) — file cũ tự
  bị đẩy ra, URL cũ sẽ trả 404 sau vài chục lượt tạo mới.
- Free tier Render giới hạn 750 giờ chạy/tháng cho toàn bộ tài khoản — 1 service
  chạy 24/7 hết khoảng 730 giờ/tháng, vẫn nằm trong hạn mức nếu chỉ chạy 1 service.
