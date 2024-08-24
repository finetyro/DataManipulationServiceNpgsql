using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using Npgsql;
using System.Transactions;
using Newtonsoft.Json;

namespace ru.finetyro.data.npgsql.manipulate
{
    public class ColumnMetaData
    {
        public string ColumnName { get; set; }
        public object ColumnValue { get; set; }
        public DbType ColumnType { get; set; }
    }

    public class SpecificColumn
    {
        public string ColumnName { get; }
        public DbType ColumnType { get; }
        public SpecificDataSet DataSet { get; }

        public SpecificDataSet Set(object colValue)
        {
            DataSet.GetMetaData().AddColumn(ColumnName, colValue);
            return DataSet;
        }

        public SpecificColumn(string columnName, SpecificDataSet dataSet)
        {
            ColumnName = columnName;
            DataSet = dataSet;
            ColumnType = DbType.Object;
        }

        public SpecificColumn(string columnName, DbType columnType, SpecificDataSet dataSet)
        {
            ColumnName = columnName;
            DataSet = dataSet;
            ColumnType = columnType;
        }
    }

    public class SpecificKey
    {
        public string KeyName { get; }
        public DbType KeyType { get; }
        public SpecificDataSet DataSet { get; }

        public SpecificDataSet Set(object colValue, [CallerMemberName] string memberName = "")
        {
            DataSet.GetMetaData().AddKey(KeyName, colValue);
            return DataSet;
        }

        public SpecificKey(string keyName, SpecificDataSet dataSet)
        {
            KeyName = keyName;
            DataSet = dataSet;
            KeyType = DbType.Object;
        }

        public SpecificKey(string keyName, DbType keyType, SpecificDataSet dataSet)
        {
            KeyName = keyName;
            DataSet = dataSet;
            KeyType = keyType;
        }
    }

    public class DataSetMetaData
    {
        public TransactionOptions _transactionOptions { get; set; }

        private bool InitializeTable;
        public bool IsTransaction { get; set; }
        public TransactionScopeOption _tranScopeOption { get; set; }
        public List<ColumnMetaData> _columns { get; }
        public List<ColumnMetaData> _keys { get; }
        public string SchemaName { get; private set; }
        public string TableName { get; private set; }

        public void SetTable(string schemaName, string tableName)
        {
            if (!InitializeTable)
            {
                SchemaName = schemaName;
                TableName = tableName;
            }
            else
                throw new Exception("Table metadata already initialized");
        }

        public DataSetMetaData()
        {
            _columns = new List<ColumnMetaData>();
            _keys = new List<ColumnMetaData>();
            InitializeTable = false;
            IsTransaction = false;
            _tranScopeOption = TransactionScopeOption.Required;
            _transactionOptions = new TransactionOptions
            {
                IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted,
                Timeout = TimeSpan.FromSeconds(30)
            };
        }

        public void AddColumn(string colName, object colValue)
        {
            _columns.Add(new ColumnMetaData { ColumnName = colName, ColumnType = DbType.Object, ColumnValue = colValue });
        }

        public void AddColumn(string colName, DbType colType, object colValue)
        {
            _columns.Add(new ColumnMetaData { ColumnName = colName, ColumnType = colType, ColumnValue = colValue });
        }

        public void AddKey(string keyName, object keyValue)
        {
            _keys.Add(new ColumnMetaData { ColumnName = keyName, ColumnType = DbType.Object, ColumnValue = keyValue });
        }

        public void AddKey(string keyName, DbType keyType, object keyValue)
        {
            _keys.Add(new ColumnMetaData { ColumnName = keyName, ColumnType = keyType, ColumnValue = keyValue });
        }
    }

    public class SpecificDataSet : IDisposable
    {
        private IDbConnection _dbConnection;
        private readonly DataSetMetaData _metaData;


        public DataSetMetaData GetMetaData()
        {
            return _metaData;
        }

        public SpecificDataSet For(IDbConnection connection)
        {
            return this;
        }

        public SpecificDataSet(IDbConnection connection)
        {
            _dbConnection = connection;
            if (!connection.State.HasFlag(ConnectionState.Open))
                connection.Open();
            _metaData = new DataSetMetaData();

        }

        public SpecificDataSet(IDbConnection connection, bool useReadyMade)
        {
            _dbConnection = connection;
            _metaData = new DataSetMetaData();
            if (!useReadyMade)
                if (!connection.State.HasFlag(ConnectionState.Open))
                    connection.Open();
        }

        public SpecificDataSet()
        {
            _metaData = new DataSetMetaData();
        }

        public SpecificDataSet Truncate()
        {
            var sql = $"truncate table {_metaData.SchemaName}.{_metaData.TableName};";
            ExecuteSql(sql);
            return this;
        }

        private string GetKeysWhere()
        {
            if (_metaData._keys.Any())
            {
                var keysCommands = _metaData._keys
                    .ToList()
                    .Select(it => $"{it.ColumnName} = :{it.ColumnName}_cnd_p");
                var strKeys = string.Join(" and ", keysCommands);
                return strKeys;
            }
            else
                return "true";
        }

        private DbParameter[] GetKeysParameters()
        {
            return _metaData._keys
                        .ToList()
                        .Select(it => new NpgsqlParameter { DbType = it.ColumnType, ParameterName = $"{it.ColumnName}_cnd_p", Value = it.ColumnValue })
                        .ToArray();
        }

        private DbParameter[] GetColsParameters()
        {
            return _metaData._columns
                        .ToList()
                        .Select(it => new NpgsqlParameter { DbType = it.ColumnType, ParameterName = $"{it.ColumnName}_p", Value = it.ColumnValue })
                        .ToArray();
        }

        public string GetPreparedUpdateQuery()
        {
            var cols = _metaData._columns.ToList();
            var colsCommands = cols.Select(it => $"{it.ColumnName} = :{it.ColumnName}_p");
            var strCols = string.Join(", ", colsCommands);
            var sql = $@"update {_metaData.SchemaName}.{_metaData.TableName} set {strCols}
                     where {GetKeysWhere() ?? "true"}";
            return sql;
        }

        public SpecificDataSet Table(string tableName)
        {
            GetMetaData().SetTable("public", tableName);
            return this;
        }

        public SpecificDataSet Table(string schemaName, string tableName)
        {
            GetMetaData().SetTable(schemaName, tableName);
            return this;
        }

        public SpecificColumn Column(string columnName)
        {
            return new SpecificColumn(columnName, this);
        }

        public SpecificColumn Column(string columnName, DbType columnType)
        {
            return new SpecificColumn(columnName, columnType, this);
        }

        public SpecificKey WithKeys(string keyColumnName)
        {
            return new SpecificKey(keyColumnName, this);
        }

        public SpecificColumn WithKeys(string keyColumnName, DbType keyColumnType)
        {
            return new SpecificColumn(keyColumnName, keyColumnType, this);
        }

        public SpecificDataSet WithTransaction(System.Transactions.IsolationLevel isolationLevel, int timeoutSeconds, TransactionScopeOption transactionOption)
        {
            _metaData._tranScopeOption = transactionOption;
            _metaData._transactionOptions = new TransactionOptions
            {
                IsolationLevel = isolationLevel,
                Timeout = TimeSpan.FromSeconds(timeoutSeconds)
            };
            _metaData.IsTransaction = true;
            return this;
        }

        public SpecificDataSet Insert()
        {
            SpecificDataSet Do()
            {
                var cols = _metaData._columns.ToList();
                var colsForInsert = cols.Select(it => it.ColumnName);
                var strCols = string.Join(", ", colsForInsert);
                var valuesForInsert = cols.Select(it => $":{it.ColumnName}_p");
                var strValues = string.Join(", ", valuesForInsert);
                var sql = $"insert into {_metaData.SchemaName}.{_metaData.TableName} ({strCols}) values ({strValues})";
                ExecuteSql(sql, GetColsParameters());
                return this;
            };

            if (_metaData.IsTransaction)
            {
                using (TransactionScope scope = new TransactionScope(_metaData._tranScopeOption, _metaData._transactionOptions))
                {
                    Do();
                }
            }
            else
                Do();
            return this;
        }

        public SpecificDataSet Update()
        {
            if (_metaData.IsTransaction)
            {
                using (TransactionScope scope = new TransactionScope(_metaData._tranScopeOption, _metaData._transactionOptions))
                {
                    ExecuteSql(GetPreparedUpdateQuery(), GetKeysParameters().Concat(GetColsParameters()).ToArray());
                }
            }
            else
                ExecuteSql(GetPreparedUpdateQuery(), GetKeysParameters().Concat(GetColsParameters()).ToArray());
            return this;
        }

        public SpecificDataSet Delete()
        {
            SpecificDataSet Do()
            {
                var sql = $"delete from {_metaData.SchemaName}.{_metaData.TableName} where {GetKeysWhere()}";
                ExecuteSql(sql, GetKeysParameters());
                return this;
            };

            if (_metaData.IsTransaction)
            {
                using (TransactionScope scope = new TransactionScope(_metaData._tranScopeOption, _metaData._transactionOptions))
                {
                    Do();
                }
            }
            else
                Do();
            return this;
        }

        private void ExecuteSql(string sql, DbParameter[] parameters = null)
        {
            using (IDbCommand command = _dbConnection.CreateCommand())
            {
                command.CommandText = sql;
                if (parameters != null)
                {
                    foreach (NpgsqlParameter parameter in parameters)
                    {
                        command.Parameters.Add(parameter);
                    };
                }
                command.ExecuteNonQuery();
            }
        }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(_metaData, Newtonsoft.Json.Formatting.Indented);
        }

        public void Dispose()
        {
            _dbConnection.Dispose();
        }
    }
}