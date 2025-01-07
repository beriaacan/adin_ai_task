# Adin AI Task - FastAPI

Bu proje, **FastAPI** kullanarak `tbl_daily_campaigns` ve `tbl_daily_scores` tablolarından verileri birleştirir ve belirtilen formatta JSON çıktısı üretir.

## Kurulum

1. Projeyi klonla veya indir:
   ```bash
   git clone <REPO_URL>
   cd adin_ai_task   
4. Gerekli kütütphaneleri yükle
   pip install -r requirements.txt

6. Run komutu
   uvicorn main:app --host 0.0.0.0 --port 8000 --reload

8. Kullanım
   Swagger Dokümanı: http://127.0.0.1:8000/docs
   Örnek İstek:
   GET /api/campaigns?campaign_id=123&start_date=2023-01-01&end_date=2023-02-01

