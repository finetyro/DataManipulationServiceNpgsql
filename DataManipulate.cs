using System.Data.Common;
using System.Data;
using System.Runtime.CompilerServices;
using Npgsql;
using System.Transactions;
using Newtonsoft.Json;
using System.Dynamic;
using System.Collections.Generic;
using System.Linq;
using System;

namespace ru.finetyro.data.npgsql.manipulate
{
    public class ColumnMetaData
    {
        public string ColumnName { get; set; }
        public object ColumnValue { get; set; }
        public DbType ColumnType { get; set; }
        public string ColumnAlias { get; set; }
    }

    public class SpecificColumn
    {
        public string ColumnName { get; }
        public DbType ColumnType { get; }
        public SpecificDataSet DataSet { get; }
        public string ColumnAlias { get; set; }

        public SpecificDataSet Set(object colValue)
        {
            DataSet.GetMetaData().AddColumn(ColumnName, colValue, ColumnAlias ?? ColumnName);
            return DataSet;
        }

        public SpecificColumn As(string alias)
        {
            ColumnAlias = alias;
            return this;
        }

        public SpecificDataSet Get()
        {
            DataSet.GetMetaData().AddColumn(ColumnName, null, ColumnAlias ?? ColumnName);
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
        public TransactionOptions TransactionOptions { get; set; }

        private bool InitializeTable;
        public bool IsTransaction { get; set; }
        public TransactionScopeOption TranScopeOption { get; set; }
        public List<ColumnMetaData> Columns { get; }
        public List<ColumnMetaData> Keys { get; }
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
            Columns = new List<ColumnMetaData>();
            Keys = new List<ColumnMetaData>();
            InitializeTable = false;
            IsTransaction = false;
            TranScopeOption = TransactionScopeOption.Required;
            TransactionOptions = new TransactionOptions
            {
                IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted,
                Timeout = TimeSpan.FromSeconds(30)
            };
        }

        public void AddColumn(string colName, object colValue, string alias)
        {
            Columns.Add(new ColumnMetaData { ColumnName = colName, ColumnType = DbType.Object, ColumnValue = colValue, ColumnAlias = alias });
        }

        public void AddKey(string keyName, object keyValue)
        {
            Keys.Add(new ColumnMetaData { ColumnName = keyName, ColumnType = DbType.Object, ColumnValue = keyValue });
        }

        public void AddKey(string keyName, DbType keyType, object keyValue)
        {
            Keys.Add(new ColumnMetaData { ColumnName = keyName, ColumnType = keyType, ColumnValue = keyValue });
        }
    }

    public class SpecificDataSet : IDisposable
    {
        private IDbConnection _dbConnection;
        private DataSetMetaData _metaData;

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
            CheckExistsTableName();
            var sql = $"truncate table {_metaData.SchemaName}.{_metaData.TableName};";
            ExecuteSql(sql);
            return this;
        }

        private string GetKeysWhere()
        {
            if (_metaData.Keys.Any())
            {
                var keysCommands = _metaData.Keys
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
            return _metaData.Keys
                        .ToList()
                        .Select(it => new NpgsqlParameter { DbType = it.ColumnType, ParameterName = $"{it.ColumnName}_cnd_p", Value = it.ColumnValue })
                        .ToArray();
        }

        private DbParameter[] GetColsParameters()
        {
            return _metaData.Columns
                        .ToList()
                        .Select(it => new NpgsqlParameter { DbType = it.ColumnType, ParameterName = $"{it.ColumnName}_p", Value = it.ColumnValue })
                        .ToArray();
        }

        public string GetPreparedUpdateQuery()
        {
            var cols = _metaData.Columns.ToList();
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
            _metaData.TranScopeOption = transactionOption;
            _metaData.TransactionOptions = new TransactionOptions
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
                var cols = _metaData.Columns.ToList();
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
                using (TransactionScope scope = new TransactionScope(_metaData.TranScopeOption, _metaData.TransactionOptions))
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
            CheckExistsTableName();
            if (_metaData.IsTransaction)
            {
                using (TransactionScope scope = new TransactionScope(_metaData.TranScopeOption, _metaData.TransactionOptions))
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
            CheckExistsTableName();
            SpecificDataSet Do()
            {
                var sql = $"delete from {_metaData.SchemaName}.{_metaData.TableName} where {GetKeysWhere()}";
                ExecuteSql(sql, GetKeysParameters());
                return this;
            };

            if (_metaData.IsTransaction)
            {
                using (TransactionScope scope = new TransactionScope(_metaData.TranScopeOption, _metaData.TransactionOptions))
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

        private List<dynamic> SqlRead(string sql, DbParameter[] parameters = null)
        {
            var colAliasDict = this._metaData.Columns
                .ToDictionary(it => it.ColumnName, it => it.ColumnAlias);

            List<dynamic> results = new List<dynamic>();
            using (var command = new NpgsqlCommand(sql, (NpgsqlConnection)_dbConnection))
            {
                if (parameters != null)
                {
                    foreach (NpgsqlParameter parameter in parameters)
                    {
                        command.Parameters.Add(parameter);
                    };
                }

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        dynamic expando = new ExpandoObject();
                        var expandoDict = (IDictionary<string, object>)expando;
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var key = reader.GetName(i);
                            string columnName = colAliasDict.ContainsKey(key) ? colAliasDict[key] : key;
                            object value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                            expandoDict[columnName] = value;
                        }
                        results.Add(expando);
                    }
                }
            }
            return results;
        }

        private string GetColsForSelect()
        {
            var cols = _metaData.Columns.ToList();
            var colsForSelect = cols.Select(it => it.ColumnName);
            return string.Join(", ", colsForSelect);
        }

        public List<dynamic> Select()
        {
            CheckExistsTableName();
            var sql = $@"select {GetColsForSelect()} from {_metaData.SchemaName}.{_metaData.TableName} 
                         where {GetKeysWhere()}";
            return SqlRead(sql, GetKeysParameters());
        }

        public List<dynamic> Select(string query, DbParameter[] parameters = null)
        {
            CheckExistsTableName();
            if (_metaData.Columns.Any()) query = $"select {GetColsForSelect()} from ({query}) d";
            if (_metaData.Keys.Any()) query = string.Concat(query, $" where {GetKeysWhere()} ");
            return SqlRead(query, parameters ?? new NpgsqlParameter[] { }.Concat(GetKeysParameters()).ToArray());
        }

        public SpecificDataSet ClearMetaData()
        {
            _metaData = new DataSetMetaData();
            return this;
        }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(_metaData, Newtonsoft.Json.Formatting.Indented);
        }

        public void Dispose()
        {
            _dbConnection.Dispose();
        }

        private void CheckExistsTableName()
        {
            if (_metaData == null) throw new Exception("Missing meta data!");
            if (_metaData.TableName == null) throw new Exception("Missing table name!");
        }
    }
}