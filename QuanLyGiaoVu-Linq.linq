<Query Kind="Expression">
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
Hocviens.Join(Lops, hv => hv.Malop, lop => lop.Malop, (hv, lop) => new {hv,lop}).
Where(x => x.hv.Mahv == x.lop.Trglop).Select(x => new {x.hv.Mahv, x.hv.Ho, x.hv.Ten, x.hv.Ngsinh, x.hv.Malop})