<Query Kind="Statements">
  <Connection>
    <ID>96ddb991-7d32-4e51-8907-36742c7bbdc4</ID>
    <NamingServiceVersion>2</NamingServiceVersion>
    <Persist>true</Persist>
    <Driver Assembly="(internal)" PublicKeyToken="no-strong-name">LINQPad.Drivers.EFCore.DynamicDriver</Driver>
    <Server>localhost</Server>
    <Database>Linq</Database>
    <UserName>postgres</UserName>
    <SqlSecurity>true</SqlSecurity>
    <Password>AQAAANCMnd8BFdERjHoAwE/Cl+sBAAAAqkYvVMa0GEecH93gw+e6cAAAAAACAAAAAAAQZgAAAAEAACAAAACuHIPaMpwEDAVFuxmraxxBnTEm5jYsVaa9fual9/VriwAAAAAOgAAAAAIAACAAAADJIVt/SpG7lZxRjF4WliLuE8A6ismgXXfPOVjauqn3rxAAAAACXgBnfBvZOVVHOlB71xBUQAAAAJBHQypBrqyjp6sC6h3aFWedhy42j0finGwqaSV9zU2PEb7GrvSA04mq7z+efoiXIiHxJVZrT24ypPgNXFsL3i8=</Password>
    <AllowDateOnlyTimeOnly>true</AllowDateOnlyTimeOnly>
    <IsProduction>true</IsProduction>
    <DriverData>
      <Port>5432</Port>
      <EncryptSqlTraffic>True</EncryptSqlTraffic>
      <PreserveNumeric1>True</PreserveNumeric1>
      <EFProvider>Npgsql.EntityFrameworkCore.PostgreSQL</EFProvider>
    </DriverData>
  </Connection>
</Query>

//1 In ra danh sách (mã học viên, họ tên, ngày sinh, mã lớp) lớp trưởng của các lớp
Hocviens.Join(
			Lops,
			hv => hv.Malop, 
			lop => lop.Malop, 
			(hv, lop) => new {hv,lop})
				.Where(x => x.hv.Mahv == x.lop.Trglop)
					.Select(x => new 
					{
						x.hv.Mahv, 
						x.hv.Ho, 
						x.hv.Ten, 
						x.hv.Ngsinh, 
						x.hv.Malop
					}
			);
//2 In ra bảng điểm khi thi (mã học viên, họ tên , lần thi, điểm số) môn CTRR của lớp “K12”, sắp xếp theo tên, họ học viên
Ketquathis
.Join(
	Hocviens,
	kq => kq.Mahv,
	hv => hv.Mahv,
	(kq, hv) => new 
		{
			MaHV = hv.Mahv,
			HoTen = hv.Ho + " " + hv.Ten,
			LanThi = kq.Lanthi,
			Diem = kq.Diem,
			MaMH = kq.Mamh,
			MaLop = hv.Malop
		}
).Where(
	x => x.MaMH == "CTRR" &&
	x.MaLop == "K12"
);
//3 In ra danh sách những học viên (mã học viên, họ tên) và những môn học mà học viên đó thi lần thứ nhất đã đạt
Ketquathis.
Join(
	Hocviens,
	kq => kq.Mahv,
	hv => hv.Mahv,
	(kq, hv) => new {kq, hv}
)
.Join(
	Monhocs,
	x => x.kq.Mamh,
	mh => mh.Mamh,
	(x, mh) => new {x, mh}
)
.Where(
	dk => dk.x.kq.Lanthi == 1 &&
	dk.x.kq.Kqua == "Dat"
)
.Select(
	kq => new
	{
		MaHV = kq.x.hv.Mahv,
		Ho = kq.x.hv.Ho,
		Ten = kq.x.hv.Ten,
		MaMH = kq.mh.Mamh,
		TenMH = kq.mh.Tenmh
	}
);
//4 In ra danh sách học viên (mã học viên, họ tên) của lớp “K11” thi môn CTRR không đạt (ở lần thi 1)
Ketquathis
.Join(
	Hocviens,
	kq => kq.Mahv,
	hv => hv.Mahv,
	(kq, hv) => new 
	{
		MaHV = hv.Mahv,
		HoTen = hv.Ho + " " + hv.Ten,
		MaMH = kq.Mamh,
		KQua = kq.Kqua,
		LanThi = kq.Lanthi,
		MaLop = hv.Malop
	}
)
.Where( x => 
	x.MaMH == "CTRR"
	&& x.MaLop == "K11"
	&& x.LanThi == 1
	&& x.KQua != "Dat"
)
.Select(
	x => new
	{
		MaHV = x.MaHV,
		HoTen = x.HoTen,
		MaMH = x.MaMH,
	}	
);
//5 Danh sách học viên (mã học viên, họ tên) của lớp “K” thi môn CTRR không đạt (ở tất cả các lần thi)
Hocviens
.Join(
	Ketquathis,
	hv => hv.Mahv,
	kq => kq.Mahv,
	(hv, kq) => new {hv, kq}
)
.Where( x => 
	x.hv.Malop.Contains("K") &&
	x.kq.Mamh == "CTRR" &&
	!Ketquathis.Any(kqt => 
		kqt.Mahv == x.kq.Mahv &&
		kqt.Mamh == "CTRR" &&
		kqt.Kqua == "Dat")
)
.Select(
	kq => new
	{
		MaHV = kq.hv.Mahv,
		HoTen = kq.hv.Ho + " " + kq.hv.Ten
	}
)
.ToList()
.DistinctBy(x => x.MaHV);
//6 Tìm tên những môn học mà giáo viên có tên “Tran Tam Thanh” dạy trong học kỳ 1 năm 2006
// HỌC JOIN Ở CÂU NÀY
Monhocs
.Join(
	Giangdays,
	mh => mh.Mamh,
	gd => gd.Mamh,
	(mh, gd) => new {mh, gd}
)
.Join(
	Giaoviens,
	x => x.gd.Magv,
	gv => gv.Magv,
	(x, gv) => new {x.gd, x.mh, gv}
)
.Where(dk =>
	dk.gv.Hoten == "Tran Tam Thanh" &&
	dk.gd.Hocky == 1 &&
	dk.gd.Nam == 2006
)
.Select(
	kq => new
	{
		MaMH = kq.mh.Mamh,
		TenMH = kq.mh.Tenmh,
	}
)
.ToList()
.DistinctBy(x => x.MaMH);
//7 Tìm những môn học (mã môn học, tên môn học) mà giáo viên chủ nhiệm lớp “K11” dạy trong học kỳ 1 năm 2006
Monhocs
.Join(
	Giangdays,
	mh => mh.Mamh,
	gd => gd.Mamh,
	(mh, gd) => new {mh, gd}
)
.Join(
	Lops,
	x => x.gd.Malop,
	lop => lop.Malop,
	(x, lop) => new {x.gd, x.mh, lop}
)
.Where( x =>
	x.gd.Magv == x.lop.Magvcn &&
	x.lop.Malop == "K11" &&
	x.gd.Hocky == 1 &&
	x.gd.Nam == 2006
)
.Select( x => new
	{
		MaMH = x.mh.Mamh,
		TenMH = x.mh.Tenmh
	}
)
.ToList()
.DistinctBy(x => x.MaMH);
//8 Tìm họ tên lớp trưởng của các lớp mà giáo viên có tên “Nguyen To Lan” dạy môn “Co So Du Lieu”
Hocviens
.Join(
	Lops,
	hv => hv.Malop,
	lop => lop.Malop,
	(hv, lop) => new {hv, lop}
)
.Join(
	Giangdays,
	x => x.lop.Malop,
	gd => gd.Malop,
	(x, gd) => new {x.hv, x.lop, gd}
)
.Join(
	Giaoviens,
	x => x.gd.Magv,
	gv => gv.Magv,
	(x, gv) => new {x.hv, x.lop, x.gd, gv}
)
.Join(
	Monhocs,
	x => x.gd.Mamh,
	mh => mh.Mamh,
	(x, mh) => new {x.hv, x.lop, x.gd, x.gv, mh}
)
.Where( x =>
	x.gv.Hoten == "Nguyen To Lan" &&
	x.mh.Tenmh == "Co So Du Lieu"
)
.Select(
	x => new
	{
		MaHV = x.hv.Mahv,
		TenHV = x.hv.Ho + " " + x.hv.Ten,
		Lop = x.lop.Malop,
		HocKy = x.gd.Hocky,
		NamHoc = x.gd.Nam
	}
);
//11 Tìm họ tên giáo viên dạy môn CTRR cho cả hai lớp “K11” và “K12” trong cùng học kỳ 1 năm 2006
Giangdays
.Where(x => 
	x.Malop == "K11" &&
	x.Hocky == 1 &&
	x.Nam == 2006 
).ToList()
.IntersectBy(
	Giangdays
	.Where(x => 
		x.Malop == "K12" &&
		x.Hocky == 1 &&
		x.Nam == 2006 
	)
	.Select(x => x.Magv),
	x => x.Magv
).ToList()
.Join(
	Giaoviens,
	gd => gd.Magv,
	gv => gv.Magv,
	(gd, gv) => new 
	{
		MaGV = gv.Magv,
		TenGv = gv.Hoten
	}
)
//12 Tìm những học viên (mã học viên, họ tên) thi không đạt môn CSDL ở lần thi thứ 1 nhưng chưa thi lại môn này
Hocviens
.Join(Ketquathis,
	hv => hv.Mahv,
	kqt => kqt.Mahv,
	(hv, kqt) => new {hv, kqt}
)
.Where( x =>
	x.kqt.Lanthi == 1 &&
	x.kqt.Kqua != "Dat" &&
	x.kqt.Mamh == "CSDL" &&
	!Ketquathis.Any(kqt => 
		kqt.Mahv == x.kqt.Mahv &&
		kqt.Lanthi > 1
	)
)
.Select( 
	x => new
	{
		MaHV = x.hv.Mahv,
		TenHV = x.hv.Ho + " " + x.hv.Ten
	}
);
//13 Tìm giáo viên (mã giáo viên, họ tên) không được phân công giảng dạy bất kỳ môn học nào
Giaoviens
.GroupJoin(Giangdays,
    gv => gv.Magv,
    gd => gd.Magv,
    (gv, gds) => new { gv, gds} //left Join
)
.Where(x => !x.gds.Any())
.Select(x => new
{
    MaGV = x.gv.Magv,
    TenGV = x.gv.Hoten
})
.ToList();
//14 Tìm giáo viên (mã giáo viên, họ tên) không được phân công giảng dạy bất kỳ môn học nào thuộc khoa giáo viên đó phụ trách
//CÂU NÀY KHÓ
Giaoviens
.Where(gv => !Monhocs
    .Where(mh => mh.Makhoa == gv.Makhoa)
    .Any(mh =>
        !Giangdays.Any(gd =>
            gd.Mamh == mh.Mamh && gd.Magv == gv.Magv
        )
    )
)
.Select(gv => new
{
    MaGV = gv.Magv,
    TenGV = gv.Hoten
})
.ToList();

//15 Tìm họ tên các học viên thuộc lớp “K11” thi một môn bất kỳ quá 3 lần vẫn “Khong dat” hoặc thi lần thứ 2 môn CTRR được 5 điểm
Hocviens
.Join(Ketquathis,
	hv => hv.Mahv,
	kqt => kqt.Mahv,
	(hv, kqt) => new {hv, kqt}
)
.Where(x => 
	x.hv.Malop == "K11" &&
	((x.kqt.Lanthi == 2 && x.kqt.Mamh == "CTRR" && x.kqt.Diem == 5) || 
	Ketquathis.Where(z => z.Kqua != "Dat")
	.GroupBy(z => new {z.Mahv, z.Mamh})
	.Where(g => g.Count() > 3)
	.Select(g => g.Key.Mahv)
	.Contains(x.hv.Mahv)
))
.Select(
	x => new
	{
		HoTen = x.hv.Ho + " " + x.hv.Ten
	}
). Distinct();

//16 Tìm họ tên giáo viên dạy môn CTRR cho ít nhất hai lớp trong cùng một học kỳ của một năm học
Giaoviens
    .Where(gv => 
		Giangdays.Any(gd => 
			gv.Magv == gd.Magv && 
			gd.Mamh == "CTRR" &&
			Giangdays
			.Any(gd1 =>
				gd1.Magv == gd.Magv &&
				gd1.Mamh == "CTRR" &&
				gd1.Malop != gd.Malop &&
				gd1.Hocky == gd.Hocky &&
				gd1.Nam == gd.Nam
			)
		)
	)
	.Select(x =>
		new {
			MaGV = x.Magv,
			TenGV = x.Hoten
		}
	);
//17 Danh sách học viên và điểm thi môn CSDL (chỉ lấy điểm của lần thi sau cùng)
Hocviens
.Join(
	Ketquathis,
	hv => hv.Mahv,
	kqt => kqt.Mahv,
	(hv, kqt) => new {hv, kqt}
)
.Where(x =>
	x.kqt.Mamh == "CSDL" &&
	x.kqt.Lanthi == 
	Ketquathis
	.Where(y => 
		y.Mahv == x.hv.Mahv &&
		y.Mamh == "CSDL")
	.Max(m => m.Lanthi)	
	)
.Select(x =>
	new {
		MaHV = x.hv.Mahv,
		TenHV = x.hv.Ho + " " + x.hv.Ten,
		Mamh = x.kqt.Mamh,
		Diem = x.kqt.Diem
	}
);
//18 Danh sách học viên và điểm thi môn “Co So Du Lieu” (chỉ lấy điểm cao nhất của các lần thi)
Hocviens
.Join(
	Ketquathis,
	hv => hv.Mahv,
	kqt => kqt.Mahv,
	(hv, kqt) => new {hv, kqt}
)
.Join(
	Monhocs,
	x => x.kqt.Mamh,
	mh => mh.Mamh,
	(x, mh) => new {x.hv, x.kqt, mh}
)
.Where(x =>
	x.mh.Tenmh == "Co So Du Lieu" &&
	x.kqt.Lanthi == 
	Ketquathis
	.Join(
		Monhocs,
		kqt => kqt.Mamh,
		mh => mh.Mamh,
		(kqt, mh) => new {kqt, mh}
	)
	.Where(y => 
		y.kqt.Mahv == x.hv.Mahv &&
		y.mh.Tenmh == "Co So Du Lieu")
	.Max(m => m.kqt.Lanthi)	
	)
.Select(x =>
	new {
		MaHV = x.hv.Mahv,
		TenHV = x.hv.Ho + " " + x.hv.Ten,
		Mamh = x.kqt.Mamh,
		Tenmh = x.mh.Tenmh,
		Diem = x.kqt.Diem
	}
);
//19 Khoa nào (mã khoa, tên khoa) được thành lập sớm nhất
Khoas
.Where(x =>
	x.Ngtlap == Khoas.Min(x => x.Ngtlap)
)
.Select(x => 
	new {
		MaKhoa = x.Makhoa,
		TenKhoa = x.Tenkhoa
	}
)
//20 Có bao nhiêu giáo viên có học hàm là “GS” hoặc “PGS”
Giaoviens
.Where(x => 
	x.Hocham == "GS" ||
	x.Hocham == "PGS"
)
.Count()
//21 Thống kê có bao nhiêu giáo viên có học vị là “CN”, “KS”, “Ths”, “TS”, “PTS” trong mỗi khoa
Giaoviens
    .GroupBy(gv => new { gv.Makhoa, gv.Hocvi })
    .Select(group => new 
    {
        MaKhoa = group.Key.Makhoa,
        HocVi = group.Key.Hocvi,
        SoGiaoVien = group.Count()
    })
    .OrderBy(x => x.MaKhoa)
    .ToList();
//22 Mỗi môn học thống kê số lượng học viên theo kết quả (đạt và không đạt)
Monhocs
.Join(
	Ketquathis,
	mh => mh.Mamh,
	kqt => kqt.Mamh,
	(mh, kqt) => new {mh, kqt}
)
.GroupBy(gr =>
	new {
		gr.mh.Mamh,
		gr.mh.Tenmh,
		gr.kqt.Kqua
	}
)
.OrderBy(or => or.Key.Mamh)
.Select(sl =>
	new {
		MaMH = sl.Key.Mamh,
		TenMH = sl.Key.Tenmh,
		Kqua = sl.Key.Kqua,
		Count = sl.Count()
	}
);
//23 Tìm giáo viên (mã giáo viên, họ tên) là giáo viên chủ nhiệm của một lớp, đồng thời dạy cho lớp đó ít nhất một môn học.
Giaoviens
.Where(gv =>
	Giangdays
	.Any(gd => 
		gd.Magv == gv.Magv &&
		Lops.Any(l =>
			l.Magvcn == gv.Magv &&
			l.Malop == gd.Malop
		)
	)
)
.Select(sl =>
	new 
	{
		MaGV = sl.Magv,
		TenGV = sl.Hoten,
	}
)
.AsEnumerable()
.DistinctBy(dt => dt.MaGV);
//24 Tìm họ tên lớp trưởng của lớp có sỉ số cao nhất
Hocviens
.Where(hv => 
	Lops
	.Any(l =>
		l.Trglop == hv.Mahv &&
		l.Siso == 
		Lops.Max(m => m.Siso)
	)
)
.Select(sl => 
	new {
		MaHV = sl.Mahv,
		HoTen = sl.Ho + " " + sl.Ten 
	}
)
//25 Tìm họ tên những LOPTRG thi không đạt quá 3 môn (mỗi môn đều thi không đạt ở tất cả các lần thi)
Hocviens
.Where(hv =>
	Lops.Any(l =>
		l.Trglop == hv.Mahv
	)
	&&
	Ketquathis.Where(kqt => 
		kqt.Mahv == hv.Mahv &&
		kqt.Kqua != "Dat" 
	)
	.GroupBy(gr => new {
		gr.Mahv,
		gr.Mamh
	})
	.Any(w =>
		w.Count() > 3
	)
)
.Select(sl => 
	new {
		MaHV = sl.Mahv,
		HoTen = sl.Ho + " " + sl.Ten 
	}
)
//26 Tìm học viên (mã học viên, họ tên) có số môn đạt điểm 9,10 nhiều nhất
Hocviens
.Join(
	Ketquathis,
	hv => hv.Mahv,
	kqt => kqt.Mahv,
	(hv, kqt) => new {hv, kqt}
)
.AsEnumerable()
.GroupBy(gr =>
	new {
		gr.hv.Mahv, gr.hv.Ho, gr.hv.Ten
	}
)
.Where(hv =>
	hv.Count() == 
	Ketquathis
	.Where(kqt => 
		kqt.Diem >= 9
	)
	.GroupBy(kqt => kqt.Mahv)
	.AsEnumerable()
    .Max(g => g.Count())
)
.Select(g => new {
    MaHV = g.Key.Mahv,
    HoTen = g.Key.Ho + " " + g.Key.Ten
})
//28 Trong từng học kỳ của từng năm, mỗi giáo viên phân công dạy bao nhiêu môn học, bao nhiêu lớp.
//CÂU NÀY CHƯA XONG
Giangdays
.Join(
	Giaoviens,
	gd => gd.Magv,
	gv => gv.Magv,
	(gd, gv) => new {gd, gv}
)
.GroupBy(gr =>
	new {
		gr.gd.Nam,
		gr.gd.Hocky,
		gr.gv.Magv,
		gr.gv.Hoten,
		gr.gd.Mamh,
	}
).
Select(sl =>
	new {
		NamHoc = sl.Key.Nam,
		HocKy = sl.Key.Hocky,
		MaGV = sl.Key.Magv,
		TenGV = sl.Key.Hoten,
		SoLop = sl.Count()
	}
);
//30 Tìm môn học (mã môn học, tên môn học) có nhiều học viên thi không đạt (ở lần thi thứ 1) nhất.
Monhocs
.GroupJoin(
    Ketquathis,
    mh => mh.Mamh,
    kqt => kqt.Mamh,
    (mh, kqts) => new {
        MonHoc = mh,
        FailedTests = kqts.Where(kqt => kqt.Lanthi == 1 && kqt.Kqua == "Khong Dat")
    }
)
.AsEnumerable()
.Where(gr => gr.FailedTests.Count() == 
Ketquathis
.Where(kqt => kqt.Lanthi == 1 && kqt.Kqua == "Khong Dat")
.GroupBy(kqt => kqt.Mamh)
.AsEnumerable()
.Max(g => g.Count())
)
.AsEnumerable()
.Select(gr => new {
    MaMH = gr.MonHoc.Mamh,
    TenMH = gr.MonHoc.Tenmh
})
.ToList();
//31 Tìm học viên (mã học viên, họ tên) thi môn nào cũng đạt (chỉ xét lần thi thứ 1)
Hocviens
.Where(hv =>
	!Ketquathis.Any(kqt =>
		kqt.Mahv == hv.Mahv &&
		kqt.Kqua == "Khong Dat" && 
		kqt.Lanthi == 1
	)
)
.Select(sl => 
	new 
	{
		MaHV = sl.Mahv,
		TenHV = sl.Ho + " " + sl.Ten
	}
)
.AsEnumerable() 
.DistinctBy(dt => dt.MaHV);
//33 Tìm học viên (mã học viên, họ tên) đã thi tất cả các môn đều đạt (chỉ xét lần thi thứ 1)
Hocviens
    .Where(hv => !Monhocs
        .Any(mh => !Ketquathis
            .Any(kqt =>
                kqt.MaMH == mh.MaMH &&
                kqt.MaHV == hv.Mahv &&
                kqt.Lanthi == 1 &&
                kqt.Kqua == "Dat"
            )
        )
    )
    .Select(hv => new {
        MaHV = hv.Mahv,
        HoTen = hv.Ho + " " + hv.Ten
    })
    .ToList();

