import streamlit as st
from elasticsearch import Elasticsearch
import pandas as pd

# Kết nối Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Thiết lập giao diện Streamlit
st.title("Tra cứu tiến độ học tập sinh viên")

# Nhập mã sinh viên (F_MASV) để tìm kiếm
masv = st.text_input("Nhập mã sinh viên (F_MASV):")

# Bảng quy đổi mã sinh viên sang khóa
khoa_mapping = {
    "B20": 46,  # K46
    "B21": 47,
    "B22": 48,
    "B23": 49,
    "B24": 50,
}

# Tổng số tín chỉ yêu cầu theo ngành và khóa
credit_requirements = {
    "MMT": {46: 156, 47: 156, 48: 161, 49: 161, 50: 161},
    "NNA": {46: 141, 47: 141, 48: 141, 49: 141, 50: 141},
}

# Giới hạn tín chỉ cho từng loại học kỳ
credits_per_semester = {1: 20, 2: 20, 3: 8}

# Thời gian học theo ngành
program_duration = {
    "MMT": {46: 13, 47: 13, 48: 13, 49: 13, 50: 13},  # MMT có 13 học kỳ
    "NNA": {46: 12, 47: 12, 48: 12, 49: 12, 50: 12},  # NNA có 12 học kỳ
}

# Hàm quy đổi điểm từ thang điểm 10 sang thang điểm 4
def convert_to_4_scale(diem):
    if diem >= 9.0:
        return 4.0  # A
    elif diem >= 8.0:
        return 3.5  # B+
    elif diem >= 7.0:
        return 3.0  # B
    elif diem >= 6.5:
        return 2.5  # C+
    elif diem >= 5.5:
        return 2.0  # C
    elif diem >= 5.0:
        return 1.5  # D+
    elif diem >= 4.0:
        return 1.0  # D
    else:
        return 0.0  # F

# Kiểm tra nếu mã sinh viên đã được nhập
if masv:
    # Tạo truy vấn Elasticsearch
    query = {
        "query": {
            "match": {
                "F_MASV": masv
            }
        },
        "size": 1000  # Lấy tối đa 1000 kết quả
    }

    # Thực hiện truy vấn
    response = es.search(index="phantich1-2024.11.29", body=query)
    hits = response["hits"]["hits"]

    # Kiểm tra nếu có kết quả
    if hits:
        st.success(f"Tìm thấy {len(hits)} kết quả liên quan đến sinh viên: {masv}")

        # Chuyển đổi kết quả thành DataFrame
        data = [hit["_source"] for hit in hits]
        df = pd.DataFrame(data)

        # Lọc các cột cần thiết
        columns_needed = ["F_MAMH", "F_TENMHVN", "F_DVHT", "F_MASV", "F_TENLOP", "NHHK", "F_DIEM2", "F_TCDTTL"]
        df_filtered = df[columns_needed]

        # Xác định khóa học dựa trên mã sinh viên
        khoa = khoa_mapping.get(masv[:3], None)

        if khoa:
            # Xác định ngành học từ tên lớp
            if df_filtered["F_TENLOP"].str.startswith("DI").any():
                nganh = "MMT"
                nganh_full_name = "Mạng máy tính & TTDL"
            elif df_filtered["F_TENLOP"].str.startswith("FL").any():
                nganh = "NNA"
                nganh_full_name = "Ngôn ngữ Anh CLC"
            else:
                nganh = "Chưa xác định"
                nganh_full_name = "Chưa xác định"

            total_credits = credit_requirements[nganh][khoa]
            total_semesters = program_duration[nganh][khoa]
            current_year = 2024  
            start_year = 2020 + (khoa - 46)  # Xác định năm bắt đầu từ K46
            years_elapsed = current_year - start_year  
            
            # Tính số học kỳ đã học
            semesters_elapsed = years_elapsed * 3  
            completed_credits = float(df_filtered["F_TCDTTL"].iloc[0])  

            # Tính số tín chỉ còn lại
            remaining_credits = total_credits - completed_credits

            # Số học kỳ còn lại theo thời gian
            remaining_semesters = total_semesters - semesters_elapsed

            # Kiểm tra tiến độ học tập
            max_credits_per_semester = 20  
            max_credits_remaining_in_time = remaining_semesters * max_credits_per_semester 

            # Tính tiến độ học tập (tỷ lệ phần trăm)
            progress_percentage = (completed_credits / total_credits) * 100

            # Quy đổi điểm sang thang điểm 4
            df_filtered["F_DIEM2"] = pd.to_numeric(df_filtered["F_DIEM2"], errors="coerce")  # Chuyển điểm thành số
            df_filtered["F_DVHT"] = pd.to_numeric(df_filtered["F_DVHT"], errors="coerce")  # Chuyển tín chỉ thành số

            # Lọc các học phần không có dấu "*" và điểm >= 4.0
            df_passed = df_filtered[(df_filtered["F_DIEM2"] >= 4.0) & (~df_filtered["F_TENMHVN"].str.contains(r'\*'))]

            # Loại trừ các môn học thuộc học kỳ NHHK: 20251 khỏi việc tính ĐTBCTL
            df_passed = df_passed[df_passed["NHHK"] != 20251]

            # Quy đổi điểm sang thang điểm 4
            df_passed["F_DIEM2"] = df_passed.apply(lambda row: convert_to_4_scale(row["F_DIEM2"]), axis=1)

            # Tính tổng điểm tích lũy và tổng tín chỉ tích lũy
            total_points = (df_passed["F_DIEM2"] * df_passed["F_DVHT"]).sum()
            total_credits_earned = df_passed["F_DVHT"].sum()

            # Tính điểm trung bình tích lũy (ĐTBCTL)
            if total_credits_earned > 0:
                dtbctl = total_points / total_credits_earned
            else:
                dtbctl = 0.0

            # Hiển thị kết quả
            st.subheader("Tiến độ học tập")
            st.write(f"**Ngành học:** {nganh_full_name}")  
            st.write(f"**Khóa học:** K{khoa}")
            st.write(f"**Tổng số tín chỉ yêu cầu:** {total_credits}")
            st.write(f"**Số tín chỉ đã hoàn thành:** {int(completed_credits)}")  # Chuyển thành số nguyên

            # Kiểm tra và hiển thị số tín chỉ còn lại nếu lớn hơn 0
            if remaining_credits > 0:
               st.write(f"**Số tín chỉ còn lại:** {int(remaining_credits)}")  # Chỉ hiển thị nếu > 0

            st.write(f"**Số học kỳ còn lại:** {remaining_semesters}")
            st.write(f"**Điểm trung bình tích lũy (ĐTBCTL):** {dtbctl:.2f}")

            # Kiểm tra và hiển thị số tín chỉ còn lại nếu lớn hơn 0
            if remaining_credits <= 0:
                st.success("**Tiến độ học tập hiện tại đã hoàn thành!**")  # Hiển thị màu xanh
            else:
                # Kiểm tra nếu số học kỳ còn lại > 0
                if remaining_credits <= max_credits_remaining_in_time:
                    status = "Đúng tiến độ"
                    progress_message = "Tiến độ học tập của bạn hiện tại là đúng tiến độ!"
                    progress_color = "green"  
                else:
                    status = "Chậm tiến độ"
                    progress_message = "Bạn đang chậm tiến độ học tập."
                    progress_color = "red"

                # Hiển thị thông báo về tiến độ học tập
                if progress_color == "green":
                    st.success(f"**Tiến độ học tập hiện tại:** ({status})")
                else:
                    st.error(f"**Tiến độ học tập hiện tại:** ({status})")

            # Sắp xếp dữ liệu và hiển thị
            df_filtered["Năm học"] = df_filtered["NHHK"].astype(str).str[:4].astype(int)  # Lấy 4 ký tự đầu (năm)
            df_filtered["Học kỳ"] = df_filtered["NHHK"].astype(str).str[4].astype(int)   # Lấy ký tự thứ 5 (học kỳ)

            # Sắp xếp dữ liệu theo Năm học và Học kỳ
            df_sorted = df_filtered.sort_values(by=["NHHK"])

            # Hiển thị bảng dữ liệu (bỏ cột số thứ tự và không hiển thị Năm học, Học kỳ)
            st.dataframe(
             df_sorted.drop(columns=["Năm học", "Học kỳ"], errors="ignore"),  # Bỏ cột nếu tồn tại
             use_container_width=True  # Tự động mở rộng bảng theo khung Streamlit
            )


        else:
            st.warning("Không tìm thấy mã sinh viên hợp lệ.")
    else:
        st.warning("Không tìm thấy kết quả cho mã sinh viên này.")

