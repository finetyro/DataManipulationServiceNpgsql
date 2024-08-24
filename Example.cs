using Npgsql;
using ru.finetyro.data.npgsql.manipulate;
using System;
using System.Transactions;

namespace NpgsqlMiniDataManipulationService
{
    internal class Example
    {
        void DoExample()
        {
            NpgsqlConnection connection = new NpgsqlConnection("your connection string =)");

            connection.Open();
            new SpecificDataSet(connection, true)
                .Table("test")
                .Truncate();

            //Insert example
            new SpecificDataSet(connection, true)
                .Table("test")
                    .Column("first_name", System.Data.DbType.String).Set("Mr. Ivanov")
                    .Column("last_name").Set("Ivan")
                    .Column("birth_date").Set(new DateTime(2000, 7, 20))
                    .Column("comment").Set("the diligent student")
                    .WithTransaction(IsolationLevel.ReadCommitted, 10, TransactionScopeOption.Required)
                .Insert();

            //Update example
            new SpecificDataSet()
                .For(connection)
                .Table("test")
                    .Column("card_number").Set(10)
                    .Column("comment").Set("the diligent student #10")
                    .WithTransaction(IsolationLevel.ReadCommitted, 10, TransactionScopeOption.Required)
                .WithKeys("card_number").Set(1)
                .Update();

            //Delete example
            new SpecificDataSet(connection, true)
                .Table("public", "test")
                    .WithTransaction(IsolationLevel.Serializable, 10, TransactionScopeOption.Required)
                .WithKeys("card_number").Set(10)
                .Delete()
                .Dispose();
        }
    }
}