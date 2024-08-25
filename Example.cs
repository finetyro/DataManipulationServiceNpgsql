using Npgsql;
using ru.finetyro.data.npgsql.manipulate;
using System;
using System.Linq;
using System.Transactions;

namespace NpgsqlMiniDataManipulationService
{
    internal class Example
    {
        private const string TEST = "test";
        private const string PUBLIC = "public";
        private const string CARDNUMBER = "card_number";
        private const string LASTNAME = "last_name";
        private const string COMMENT = "comment";
        private const string CONNECTIONSTRING = "your connection string =)";
        
        void DoExample()
        {
            NpgsqlConnection connection = new NpgsqlConnection(CONNECTIONSTRING);

            connection.Open();
            new SpecificDataSet(connection, true)
                .Table(TEST)
                .Truncate();
            
            //The insert example
            new SpecificDataSet(connection, true)
                .Table(TEST)
                    .Column("first_name", System.Data.DbType.String).Set("Mr. Ivanov")
                    .Column(LASTNAME).Set("Ivan")
                    .Column("birth_date").Set(new DateTime(2000, 7, 20))
                    .Column(COMMENT).Set("the diligent student")
                    .WithTransaction(IsolationLevel.ReadCommitted, 10, TransactionScopeOption.Required)
                .Insert();

            //The update example
            new SpecificDataSet()
                .For(connection)
                .Table(TEST)
                    .Column(CARDNUMBER).Set(10)
                    .Column(COMMENT).Set("the diligent student #10")
                    .WithTransaction(IsolationLevel.ReadCommitted, 10, TransactionScopeOption.Required)
                .WithKeys(CARDNUMBER).Set(1)
                .Update();
            
            //The select example
            var students = new SpecificDataSet(connection, true)
                .Table(PUBLIC, TEST)
                    .Column(CARDNUMBER).As("CID").Get()
                    .Column(LASTNAME).As("LName").Get()
                .WithKeys(CARDNUMBER).Set(10)
                .Select();
            
            var queriedStudents = from student in students
                                select new
                                {
                                    student.CID,
                                    student.LName
                                };

            //The select example with query
            var studentsWithQuery = new SpecificDataSet(connection, true)
                .Table(PUBLIC, TEST)
                    .Column(LASTNAME).As("LName").Get()
                .Select("select * from test");

            //The delete example
            new SpecificDataSet(connection, true)
                .Table(PUBLIC, TEST)
                    .WithTransaction(IsolationLevel.Serializable, 10, TransactionScopeOption.Required)
                .WithKeys(CARDNUMBER).Set(10)
                .Delete()
                .Dispose();
        }
    }
}