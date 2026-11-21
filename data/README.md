# Dữ liệu CSE-CIC-IDS2018

Thư mục này chứa 10 file CSV gốc (khoảng 6,9 GB). Các file này không được đưa lên git.

Tải bằng AWS CLI. Bộ dữ liệu công khai nên không cần tài khoản AWS:

```powershell
aws s3 cp --no-sign-request "s3://cse-cic-ids2018/Processed Traffic Data for ML Algorithms/" data/ --recursive
```
tải bằng drive : https://drive.google.com/drive/folders/1XJNZdHXSdj48LthPQQThRHbBzvBSild1?usp=sharing

Sau khi tải xong, thư mục `data/` phải có đủ 10 file sau:

```
Wednesday-14-02-2018_TrafficForML_CICFlowMeter.csv
Thursday-15-02-2018_TrafficForML_CICFlowMeter.csv
Friday-16-02-2018_TrafficForML_CICFlowMeter.csv
Thuesday-20-02-2018_TrafficForML_CICFlowMeter.csv   (4 GB, có thêm 4 cột Flow ID / Src IP / Src Port / Dst IP)
Wednesday-21-02-2018_TrafficForML_CICFlowMeter.csv
Thursday-22-02-2018_TrafficForML_CICFlowMeter.csv
Friday-23-02-2018_TrafficForML_CICFlowMeter.csv
Wednesday-28-02-2018_TrafficForML_CICFlowMeter.csv
Thursday-01-03-2018_TrafficForML_CICFlowMeter.csv
Friday-02-03-2018_TrafficForML_CICFlowMeter.csv
```

Trang mô tả bộ dữ liệu: https://www.unb.ca/cic/datasets/ids-2018.html
