using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Dapper;

namespace DapperCMDProject
{
    class Program
    {
        static void Main(string[] args)
        {
        }
    }

    class DapperDao
    {
        private readonly string sqlConnection = "Data Source=RENFB;Initial Catalog=test;User Id=sa;Password=sa;";

        public SqlConnection OpenConnection()
        {
            SqlConnection connection = new SqlConnection(sqlConnection);
            connection.Open();
            return connection;
        }


        public List<ColumnCat> SelectColumnCats()
        {
            using (SqlConnection conn = OpenConnection())
            {
                const string query = "select * from ColumnCat order by id desc";
                return conn.Query<ColumnCat>(query, null).ToList<ColumnCat>();
            }
        }

        public string SelectColumnCatsString()
        {
            List<ColumnCat> allColumnCat = SelectColumnCats();
            StringBuilder sb = new StringBuilder();
            foreach (var cat in allColumnCat.Where(c => c.ParentId == 0))
            {
                sb.AppendLine("name==>" + cat.Name);
                sb.AppendLine("修改时间==>" + cat.ModifiedOn);
                sb.AppendLine("<br/>");
                foreach (var c in allColumnCat.Where<ColumnCat>(subColumnCat => subColumnCat.ParentId == cat.Id))
                {
                    sb.AppendLine("&nbsp;&nbsp;++++");
                    sb.AppendLine("name==>" + cat.Name);
                    sb.AppendLine("修改时间==>" + cat.ModifiedOn);
                    sb.AppendLine("<br/>");
                }
            }
            return sb.ToString();
        }

        public ColumnCat SelectColumnCat(int columnCatId)
        {
            using (IDbConnection conn = OpenConnection())
            {
                const string query = "select * from ColumnCat where Id=@id";
                return conn.Query<ColumnCat>(query, new { id = columnCatId }).SingleOrDefault<ColumnCat>();
            }
        }

        public IList<Column> SelectColumnWithColumnCat()
        {
            using (IDbConnection conn = OpenConnection())
            {
                const string query = "select c.id,c.name,c.ModifiedDate,c.ColumnCatid,cat.id,cat.[Name],cat.ModifiedOn,cat.Parentid from [Column] as c left outer join ColumnCat as cat on c.ColumnCatId=cat.id";

                return conn.Query<Column, ColumnCat, Column>(query, (column, columncat) => { column.ColumnCat = columncat; return column; }, null, null, false, "Id", null, null).ToList<Column>();
            }
        }

        public int InsertColumnCat(ColumnCat cat)
        {
            using (IDbConnection conn = OpenConnection())
            {
                const string query = "insert into ColumnCat([name],ModifiedOn,Parentid) values(@name,@ModifiedOn,@Parentid)";
                int row = conn.Execute(query, cat);
                SetIdentity(conn, id => cat.Id = id, "id", "ColumnCat");
                return row;
            }
        }

        public void SetIdentity(IDbConnection conn, Action<int> setId, string primarykey, string tableName)
        {
            if (string.IsNullOrEmpty(primarykey))
            {
                primarykey = "id";
            }
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentException("tableName参数不能为空，为查询的表名 ");
            }
            string query = string.Format("select max({0}) as Id from {1}", primarykey, tableName);
            NewId identiy = conn.Query<NewId>(query, null).Single<NewId>();

            setId(identiy.Id);
        }

        public void SetIdentity<T> (IDbConnection conn,Action<T> setId)
        {
            dynamic identity = conn.Query("select @@identity as Id").Single();
            T newId = (T)identity.Id;
            setId(newId);
        }

        public int UpdateColumnCat(ColumnCat cat)
        {
            using (IDbConnection conn=OpenConnection())
            {
                const string query = "update ColumnCat set name=@Name,ModifiedOn=@ModifiedOn,Parentid=@Parentid where Id=@id";
                return conn.Execute(query, cat);
            }
        }

        public int DeleteColumnCat(ColumnCat cat)
        {
            using (IDbConnection conn=OpenConnection())
            {
                const string query = "delete from ColumnCat where id=@id";
                return conn.Execute(query, cat);
            }
        }

        public int DeleteColumnCatAndColumn(ColumnCat cat)
        {
            using (IDbConnection conn=OpenConnection())
            {
                const string deleteColumn = "delete from [Column] where ColumnCatid=@catid";
                const string deleteColumnCat = "delete from ColumnCat where id=@id";

                IDbTransaction transaction = conn.BeginTransaction();
                try
                {
                    int row = conn.Execute(deleteColumn, new { catid = cat.Id }, transaction, null, null);
                    row += conn.Execute(deleteColumnCat, new { id = cat.Id }, transaction, null, null);
                    transaction.Commit();
                }
                catch (Exception e)
                {
                    transaction.Rollback();
                }
                return row;
            }
        }


        public class NewId
        {
            public int Id { get; set; }
        }

    }

    public class ColumnCat
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime ModifiedOn { get; set; }
        public int ParentId { get; set; }

    }

    public class Column
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime ModifiedDate { get; set; }
        public ColumnCat ColumnCat { get; set; }

    }

}
