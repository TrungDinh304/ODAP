# Hệ Thống QUản Lý Dữ Liệu Giao Dịch Thông Qua Thẻ Tín Dụng (Credit Card).

## Mô tả bài toán  

Một công ty tài chính muốn xây dựng hệ thống quản lý dữ liệu giao dịch thông qua thẻ tín dụng (credit card). Dữ liệu được phát sinh từ các máy POS đặt tại các cửa hàng mua sắm, nhà hàng, hoặc bất kỳ nơi nào thanh toán không dùng tiền mặt. Công ty muốn xây dựng hệ thống xử lý dữ liệu theo thời gian thực. Khi một giao dịch được phát sinh, dữ liệu được gửi đến hệ thống, tiến hành kiểm tra dữ liệu có lỗi hay không? Nếu `Is Fraud = Yes`, xác định lỗi và giao dịch này xem như không thành công, không cần xử lý tiếp.  

Khi một giao dịch thành công, hệ thống sẽ lưu trữ các thông tin sau:  
- **Credit Card**: Thông tin thẻ tín dụng.  
- **Ngày giao dịch**: Định dạng `dd/mm/yyyy`.  
- **Thời gian**: Định dạng `hh:mm:ss`.  
- **Merchant name**: Tên nơi xảy ra giao dịch.  
- **Merchant City**: Thành phố nơi giao dịch.  
- **Số tiền**: Chuyển sang VNĐ theo tỉ giá được cập nhật mỗi ngày.  

Cuối ngày, tất cả giao dịch sẽ được thống kê như sau:  
- Tổng giá trị giao dịch theo từng `merchant name`.  
- Đếm số lượng giao dịch của mỗi `merchant name`.  
- Thống kê theo ngày, tháng và năm.  

Tất cả thông tin thống kê này sẽ được trực quan hóa qua công cụ hoặc hệ thống chuyên biệt.

## Data Source
[Transaction-Credit card.csv](https://studenthcmusedu-my.sharepoint.com/:x:/g/personal/21120590_student_hcmus_edu_vn/ERYHCH5TKlhOhXNosy2-LAsBxtVyrlFuTWHYnC8xhzlu6A?e=WDbK5M)

## Technical Requirements
### Prerequisites
- JDK (Java Development Kit)(jdk1.8.0)
- Apache Kafka (https://kafka.apache.org/downloads)(https://kafka.apache.org/quickstart)
- Hadoop
- Apache Spark, PySpark
- AirFlow
- Streamlit

### Setup Instructions

[Run Kafka tutorial](https://studenthcmusedu-my.sharepoint.com/:b:/r/personal/pmtu_mso_hcmus_edu_vn/Documents/VNU-HCMUS/Courses/X%E1%BB%AD%20l%C3%BD%20ph%C3%A2n%20t%C3%ADch%20d%E1%BB%AF%20li%E1%BB%87u%20tr%E1%BB%B1c%20tuy%E1%BA%BFn/TH/pdf/TH%20%232%20-%20CSC17106%20%E2%80%93%20XLDLTT%20-%20Kafka.pdf?csf=1&web=1&e=tn1IDs)
[Run Hadoop tutorial for windown](https://studenthcmusedu-my.sharepoint.com/:b:/r/personal/pmtu_mso_hcmus_edu_vn/Documents/VNU-HCMUS/Courses/X%E1%BB%AD%20l%C3%BD%20ph%C3%A2n%20t%C3%ADch%20d%E1%BB%AF%20li%E1%BB%87u%20tr%E1%BB%B1c%20tuy%E1%BA%BFn/TH/pdf/TH%20%233%20-%20CSC17106%20%E2%80%93%20XLDLTT%20-%20Hadoop.pdf?csf=1&web=1&e=Kq36Vp)
[Airflow](https://studenthcmusedu-my.sharepoint.com/:b:/r/personal/pmtu_mso_hcmus_edu_vn/Documents/VNU-HCMUS/Courses/X%E1%BB%AD%20l%C3%BD%20ph%C3%A2n%20t%C3%ADch%20d%E1%BB%AF%20li%E1%BB%87u%20tr%E1%BB%B1c%20tuy%E1%BA%BFn/TH/pdf/TH%20%237%20-%20CSC17106%20%E2%80%93%20XLDLTT%20-%20Air%20Flow.pdf?csf=1&web=1&e=heQXxd)

## Chức năng và yêu cầu phân tích
1. Chức năng
   - Kafka đọc dữ liệu csv từng dòng và gửi thông tin này đến topic định nghĩa trước theo chu kì thời gian ngẫu nhiên trong phạm vi từ 1s đến 3s. 
   - Spark streaming đọc dữ liệu từ kafka theo thời gian thực, nghĩa là bất cứ thông tin nào từ kafka được xử lý tức thì, các xử lý bao gồm lọc dữ liệu, biến đổi thông tin, tính toán dữ liệu. 
   - Hadoop để lưu trữ các thông tin được xử lý từ Spark và là nơi lưu trữ thông tin được xử lý để có thể trực quan hóa dữ liệu và thống kê ở giai đoạn sau. 
   - Streamlit đọc dữ liệu từ Hadoop (dạng csv), thống kê dữ liệu theo mô tả bài toán và hiển thị dữ liệu một cách trực quan.  
   - Air Flow lên lịch quá trình đọc và hiển thị dữ liệu từ Power PI sao cho dữ liệu luôn được update mỗi ngày. 
2. Key Reports
   - Thống kê dữ liệu theo mô tả bài toán và hiển thị dữ liệu một cách trực quan.  

## Project Timeline
- Project Start: 
- Midterm Q&A: 
- Final Submission: 
- Final Q&A: 



## License
This project is created for educational purposes as part of the CSC17106 – Online Data Processing and Analysis course at University of Science - VNUHCM.

## Acknowledgments
- Course Instructors:
  - Teacher: Nguyễn Trần Minh thư
  - Teacher: Phạm Minh Tú
