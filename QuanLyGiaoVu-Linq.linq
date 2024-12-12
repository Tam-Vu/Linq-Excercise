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
//AI
Hocviens
.Join(Ketquathis,
    hv => hv.Mahv,
    kqt => kqt.Mahv,
    (hv, kqt) => new { hv, kqt })
.Where(x => x.hv.Malop == "K11" &&
            ((x.kqt.Lanthi == 2 && x.kqt.Diem == 5) ||
            Ketquathis.Where(kqt2 => kqt2.Kqua != "Dat")
                .GroupBy(kqt2 => new { kqt2.Mahv, kqt2.Mamh })
                .Where(g => g.Count() > 3)
                .Select(g => g.Key.Mahv)
                .Contains(x.hv.Mahv)))
.Select(x => new
{
    HoTen = x.hv.Ho + " " + x.hv.Ten
})
.Distinct();