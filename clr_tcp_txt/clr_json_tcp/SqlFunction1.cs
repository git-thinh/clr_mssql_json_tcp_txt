using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Net.Sockets;
using Microsoft.SqlServer.Server;
using Newtonsoft.Json;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections;

public partial class UserDefinedFunctions
{
    #region [ TCP_SEND ]

    [Microsoft.SqlServer.Server.SqlFunction()]
    public static SqlString tcp___send_text(String host, Int32 port, String text)
    {
        if (string.IsNullOrEmpty(host) || port == 0 || string.IsNullOrEmpty(text))
            return new SqlString("#ERROR: Para is null");

        try
        {
            TcpClient client = new TcpClient();
            client.Connect(host, port);
            NetworkStream stream = client.GetStream();

            byte[] buffer = System.Text.Encoding.UTF8.GetBytes(text);
            stream.Write(buffer, 0, buffer.Length);

            stream.Flush();
            stream.Close();
            client.Close();
        }
        catch (Exception ex)
        {
            return new SqlString(string.Format("#ERROR:{0}", ex.Message));
        }

        return new SqlString("#OK");
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.Read)]
    public static SqlString tcp___send_query(String host, Int32 port, String query)
    {
        if (string.IsNullOrEmpty(host) || port == 0 || string.IsNullOrEmpty(query))
            return new SqlString("#ERROR: Para is null");

        int k = 0;
        try
        {
            TcpClient client = new TcpClient();
            client.Connect(host, port);
            NetworkStream stream = client.GetStream();

            using (SqlConnection conn = new SqlConnection("context connection=true"))
            {
                conn.Open();
                SqlCommand command = new SqlCommand(query, conn);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    var columns = new List<string>();
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        columns.Add(reader.GetName(i));
                    }

                    stream.Write(new byte[] { 91 }, 0, 1); // [

                    while (reader.Read())
                    {
                        var dic = new Dictionary<string, object>() { { "ix___", k } };

                        for (var i = 0; i < reader.FieldCount; i++)
                            dic.Add(columns[i], reader.GetValue(i));

                        if (k != 0) stream.Write(new byte[] { 44 }, 0, 1); // ,

                        string json = JsonConvert.SerializeObject(dic, Formatting.Indented);
                        byte[] buffer = System.Text.Encoding.UTF8.GetBytes(json);
                        stream.Write(buffer, 0, buffer.Length);

                        k++;
                    }

                    stream.Write(new byte[] { 93 }, 0, 1); // ]

                    //while (reader.Read())
                    //{ 
                    //    string json = subfix + JsonConvert.SerializeObject(reader.GetValue(0).ToString(), Formatting.Indented);
                    //    byte[] buffer = System.Text.Encoding.UTF8.GetBytes(json);
                    //    stream.Write(buffer, 0, buffer.Length); //sends bytes to server
                    //}
                }
            }
            stream.Flush();
            stream.Close();
            client.Close();

        }
        catch (Exception ex)
        {
            return new SqlString(string.Format("#ERROR:{0}", ex.Message));
        }

        return new SqlString("#OK:" + k.ToString());
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.Read)]
    public static SqlString tcp___send_cache(String host, Int32 port, String cache_name, String query)
    {
        if (string.IsNullOrEmpty(host) || port == 0 || string.IsNullOrEmpty(query) || string.IsNullOrEmpty(cache_name))
            return new SqlString("#ERROR: Para is null");
        if (cache_name.Length > 255)
            return new SqlString("#ERROR: cache_name must be length < 256");

        int k = 0;
        try
        {
            TcpClient client = new TcpClient();
            client.Connect(host, port);
            NetworkStream stream = client.GetStream();

            using (SqlConnection conn = new SqlConnection("context connection=true"))
            {
                conn.Open();
                SqlCommand command = new SqlCommand(query, conn);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    var columns = new List<string>();
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        columns.Add(reader.GetName(i));
                    }

                    List<byte> ls = new List<byte>();
                    ls.AddRange(System.Text.Encoding.ASCII.GetBytes(cache_name));
                    int max = 257 - ls.Count;
                    for (int i = 0; i < max; i++) ls.Add(32);
                    stream.Write(ls.ToArray(), 0, ls.Count);

                    stream.Write(new byte[] { 91 }, 0, 1); // [

                    while (reader.Read())
                    {
                        var dic = new Dictionary<string, object>() { { "ix___", k } };

                        for (var i = 0; i < reader.FieldCount; i++)
                            dic.Add(columns[i], reader.GetValue(i));

                        if (k != 0) stream.Write(new byte[] { 44 }, 0, 1); // ,

                        string json = JsonConvert.SerializeObject(dic, Formatting.Indented);
                        byte[] buffer = System.Text.Encoding.UTF8.GetBytes(json);
                        stream.Write(buffer, 0, buffer.Length);

                        k++;
                    }

                    stream.Write(new byte[] { 93 }, 0, 1); // ]

                    //while (reader.Read())
                    //{ 
                    //    string json = subfix + JsonConvert.SerializeObject(reader.GetValue(0).ToString(), Formatting.Indented);
                    //    byte[] buffer = System.Text.Encoding.UTF8.GetBytes(json);
                    //    stream.Write(buffer, 0, buffer.Length); //sends bytes to server
                    //}
                }
            }
            stream.Flush();
            stream.Close();
            client.Close();
        }
        catch (Exception ex)
        {
            return new SqlString(string.Format("#ERROR:{0}", ex.Message));
        }

        return new SqlString("#OK:" + k.ToString());
    }

    static string[] arr1 = new string[] { "á", "à", "?", "ã", "?", "â", "?", "?", "?", "?", "?", "?", "?", "?", "?", "?", "?",
    "?",
    "é","è","?","?","?","ê","?","?","?","?","?",
    "í","ì","?","?","?",
    "ó","ò","?","õ","?","ô","?","?","?","?","?","?","?","?","?","?","?",
    "ú","ù","?","?","?","?","?","?","?","?","?",
    "ý","?","?","?","?"};
    static string[] arr2 = new string[] { "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a",
    "d",
    "e","e","e","e","e","e","e","e","e","e","e",
    "i","i","i","i","i",
    "o","o","o","o","o","o","o","o","o","o","o","o","o","o","o","o","o",
    "u","u","u","u","u","u","u","u","u","u","u",
    "y","y","y","y","y"};

    static string convert_Unicode_2_Ascii(string str)
    {
        if (string.IsNullOrWhiteSpace(str)) return str;

        for (int i = 0; i < arr1.Length; i++)
            str = str.Replace(arr1[i], arr2[i]);

        str = str.Replace("e?", "e").Replace("o?", "o").Replace("a?", "o");

        return str;
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.Read)]
    public static SqlString tcp___send_cache_indexs(String host, Int32 port, String cache_name, String query,
        String col_id, String col_ascii, String col_utf8, String col_org,
        String col_1n)
    {
        if (string.IsNullOrEmpty(host) || port == 0 || string.IsNullOrEmpty(query) || string.IsNullOrEmpty(cache_name))
            return new SqlString("#ERROR: Para is null");
        if (cache_name.Length > 255)
            return new SqlString("#ERROR: cache_name must be length < 256");

        int k = 0;
        try
        {
            TcpClient client = new TcpClient();
            client.Connect(host, port);
            NetworkStream stream = client.GetStream();

            bool has_all_ids = col_id == "*" ? true : false,
                 has_all_ascci = col_ascii == "*" ? true : false,
                 has_all_utf8 = col_utf8 == "*" ? true : false,
                 has_all_org = col_org == "*" ? true : false;

            string[] a_ids = string.IsNullOrEmpty(col_id) && col_id.Length > 0 ? new string[] { } : col_id.ToLower().Split(',').Select(x => x.Trim()).ToArray();
            string[] a_ascii = string.IsNullOrEmpty(col_ascii) && col_ascii.Length > 0 ? new string[] { } : col_ascii.ToLower().Split(',').Select(x => x.Trim()).ToArray();
            string[] a_utf8 = string.IsNullOrEmpty(col_utf8) && col_utf8.Length > 0 ? new string[] { } : col_utf8.ToLower().Split(',').Select(x => x.Trim()).ToArray();
            string[] a_org = string.IsNullOrEmpty(col_org) && col_org.Length > 0 ? new string[] { } : col_org.ToLower().Split(',').Select(x => x.Trim()).ToArray();
            string[] a_1n = string.IsNullOrEmpty(col_1n) && col_1n.Length > 0 ? new string[] { } : col_1n.ToLower().Split(',').Select(x => x.Trim()).ToArray();
            bool existID = false;
            long id = 0;
            bool exist1n = false;

            using (SqlConnection conn = new SqlConnection("context connection=true"))
            {
                conn.Open();
                SqlCommand command = new SqlCommand(query, conn);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    var columns = new string[reader.FieldCount];
                    var types = new string[reader.FieldCount];
                    var dtypes = new Dictionary<string, string>() { };
                    var cell = new Dictionary<string, int>() { };
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        columns[i] = reader.GetName(i).ToLower();
                        types[i] = reader.GetFieldType(i).Name.ToLower();
                        dtypes.Add(columns[i], types[i]);
                        cell.Add(columns[i], i);
                        if (columns[i] == "id" && (types[i] == "int32" || types[i] == "int64")) existID = true;
                        if (col_1n == columns[i]) exist1n = true;
                    }

                    List<byte> ls = new List<byte>();
                    ls.AddRange(Encoding.UTF8.GetBytes(cache_name));
                    int max = 257 - ls.Count;
                    for (int i = 0; i < max; i++) ls.Add(32);
                    stream.Write(ls.ToArray(), 0, ls.Count);

                    stream.Write(new byte[] { 123 }, 0, 1); // {

                    var bf = Encoding.UTF8.GetBytes(@"""items"":");
                    stream.Write(bf, 0, bf.Length);

                    var dIndex = new Dictionary<long, int>();
                    var d1n = new Dictionary<long, List<long>>();

                    var col_numbers = dtypes.Where(x => x.Key != "id" && (x.Value == "byte" || x.Value == "int32" || x.Value == "int64")).Select(x => x.Key).ToArray();
                    var col_text = dtypes.Where(x => x.Value == "string").Select(x => x.Key).ToArray();

                    stream.Write(new byte[] { 91 }, 0, 1); // [
                    while (reader.Read())
                    {
                        var dic = new Dictionary<string, object>() { { "ix___", k } };
                        for (var i = 0; i < reader.FieldCount; i++)
                            dic.Add(columns[i], reader.GetValue(i));

                        var _numbers = new List<string>();

                        if (existID)
                        {
                            switch (dtypes["id"])
                            {
                                case "int32":
                                    id = (Int32)dic["id"];
                                    break;
                                case "int64":
                                    id = (Int64)dic["id"];
                                    break;
                            }
                            dIndex.Add(id, k);
                            _numbers.Add(id.ToString());
                        }

                        if (existID && exist1n)
                        {
                            if (dic[col_1n] != null)
                            {
                                long _1n = -1;
                                switch (dtypes[col_1n])
                                {
                                    case "int32":
                                        _1n = (Int32)dic[col_1n];
                                        break;
                                    case "int64":
                                        _1n = (Int64)dic[col_1n];
                                        break;
                                }

                                if (_1n > -1)
                                {
                                    if (d1n.ContainsKey(_1n) == false) d1n.Add(_1n, new List<long>() { id });
                                    else d1n[_1n].Add(id);
                                }
                            }
                        }


                        //IEnumerable<string> _text = new string[] { };
                        var av_text = col_text.Select(x => dic[x]).Where(x => x != null).Select(x => x.ToString());
                        _numbers.AddRange(col_numbers.Select(x => dic[x]).Where(x => x != null).Select(x => x.ToString()).Where(x => x != "-1"));

                        string nums = string.Join(" ", _numbers);
                        string _text = string.Join(" ", av_text).ToLower().Trim();
                        _text = Regex.Replace(_text, @"\s+", " ");
                        string _ascii = convert_Unicode_2_Ascii(_text);

                        dic.Add("#ids", nums);
                        dic.Add("#ascii", _ascii);
                        dic.Add("#utf8", _text);

                        if (k != 0) stream.Write(new byte[] { 44 }, 0, 1); // ,
                        string json = JsonConvert.SerializeObject(dic);
                        byte[] buffer = Encoding.UTF8.GetBytes(json);
                        stream.Write(buffer, 0, buffer.Length);

                        k++;
                    }

                    stream.Write(new byte[] { 93 }, 0, 1); // ]

                    bf = System.Text.Encoding.UTF8.GetBytes(@",""columns"":" + JsonConvert.SerializeObject(dtypes)
                        + @",""indexs"":" + JsonConvert.SerializeObject(dIndex)
                        + @",""index1n"":" + JsonConvert.SerializeObject(d1n));
                    stream.Write(bf, 0, bf.Length);

                    stream.Write(new byte[] { 125 }, 0, 1); // }

                    //while (reader.Read())
                    //{ 
                    //    string json = subfix + JsonConvert.SerializeObject(reader.GetValue(0).ToString(), Formatting.Indented);
                    //    byte[] buffer = System.Text.Encoding.UTF8.GetBytes(json);
                    //    stream.Write(buffer, 0, buffer.Length); //sends bytes to server
                    //}
                }
            }
            stream.Flush();
            stream.Close();
            client.Close();
        }
        catch (Exception ex)
        {
            return new SqlString(string.Format("#ERROR:{0}", ex.Message));
        }

        return new SqlString("#OK:" + k.ToString());
    }

    #endregion

    #region [ JSON ]

    const int MAX_NVARCHAR = 4000;

    private class TableResult
    {
        public SqlInt64 Id;
        public SqlString Val;

        public TableResult(SqlInt64 id, SqlString value)
        {
            Id = id;
            Val = value;
        }
    }

    public static void TableResult_FillRow(object tResultObj, out SqlInt64 Id, out SqlString Val)
    {
        TableResult tResult = (TableResult)tResultObj;
        Id = tResult.Id;
        Val = tResult.Val;
    }

    private class KeyValResult
    {
        public SqlString Id;
        public SqlString Val;

        public KeyValResult(SqlString id, SqlString value)
        {
            Id = id;
            Val = value;
        }
    }

    public static void KeyValResult_FillRow(object kvResultObj, out SqlString Id, out SqlString Val)
    {
        KeyValResult kvResult = (KeyValResult)kvResultObj;
        Id = kvResult.Id;
        Val = kvResult.Val;
    }

    [SqlFunction(DataAccess = DataAccessKind.Read, FillRowMethodName = "TableResult_FillRow", TableDefinition = "Id bigint, Val nvarchar(max)")]
    public static IEnumerable json___query(String query)
    {
        ArrayList resultCollection = new ArrayList();
        bool hasError = false;
        StringBuilder bi = new StringBuilder();
        StringBuilder ids_error = new StringBuilder();
        string json;

        try
        {
            using (SqlConnection conn = new SqlConnection("context connection=true"))
            {
                conn.Open();
                SqlCommand command = new SqlCommand(query, conn);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    int indexID = -1;
                    var columns = new string[reader.FieldCount];
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        columns[i] = reader.GetName(i).ToLower();
                        if (columns[i] == "id") indexID = i;
                    }

                    int k = 0;
                    while (reader.Read())
                    {
                        var dic = new Dictionary<string, object>();
                        for (var i = 0; i < reader.FieldCount; i++)
                            dic.Add(columns[i], reader.GetValue(i));

                        SqlInt64 id = new SqlInt64(k);
                        if (indexID != -1) id = reader.GetSqlInt64(indexID);

                        json = JsonConvert.SerializeObject(dic);
                        if (json.Length > MAX_NVARCHAR)
                        {
                            json = ":ERROR [" + id.ToString() + "] Json length > " + MAX_NVARCHAR.ToString();
                            hasError = true;
                            bi.AppendLine(json);
                            ids_error.AppendLine(id.ToString());
                        }
                        else
                            resultCollection.Add(new TableResult(id, json));

                        k++;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            json = ":ERROR_THROW_EXCEPTION " + ex.Message;
            hasError = true;
            bi.AppendLine(json);
        }

        if (hasError)
        {
            resultCollection.Add(new TableResult(-1, bi.ToString()));
            resultCollection.Add(new TableResult(-2, ids_error.ToString()));
        }

        return resultCollection;
    }

    [SqlFunction(DataAccess = DataAccessKind.Read, FillRowMethodName = "KeyValResult_FillRow", TableDefinition = "Id nvarchar(max), Val nvarchar(max)")]
    public static IEnumerable json___table_key_value(String json)
    {
        ArrayList resultCollection = new ArrayList();
        Dictionary<string, object> vals = null;

        try
        {
            vals = JsonConvert.DeserializeObject<Dictionary<string, object>>(json);
        }
        catch (Exception ex)
        {
            resultCollection.Add(new KeyValResult(":ERROR_CONVERT_JSON", ex.Message));
        }

        if (vals != null)
        {
            foreach (var kv in vals)
            {
                if (kv.Value == null)
                {
                    resultCollection.Add(new KeyValResult(kv.Key, string.Empty));
                }
                else
                {
                    try
                    {
                        string type = kv.Value.GetType().Name.ToLower();
                        resultCollection.Add(new KeyValResult("$" + kv.Key, type));
                        resultCollection.Add(new KeyValResult(kv.Key, kv.Value as string));
                    }
                    catch (Exception ex)
                    {
                        resultCollection.Add(new KeyValResult(":ERROR [" + kv.Key + "]", ex.Message));
                    }
                }
            }
        }

        return resultCollection;
    }

    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.Read)]
    public static SqlString json___column_type(String query)
    {
        try
        {
            using (SqlConnection conn = new SqlConnection("context connection=true"))
            {
                conn.Open();
                SqlCommand command = new SqlCommand(query, conn);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    var dtypes = new Dictionary<string, string>() { };
                    var cell = new Dictionary<string, int>() { };
                    for (var i = 0; i < reader.FieldCount; i++)
                        dtypes.Add(reader.GetName(i).ToLower(), reader.GetFieldType(i).Name.ToLower());

                    string json = Newtonsoft.Json.JsonConvert.SerializeObject(dtypes, Newtonsoft.Json.Formatting.Indented);

                    return new SqlString(json);

                    //while (reader.Read())
                    //{ 
                    //    string json = subfix + JsonConvert.SerializeObject(reader.GetValue(0).ToString(), Formatting.Indented);
                    //    byte[] buffer = System.Text.Encoding.UTF8.GetBytes(json);
                    //    stream.Write(buffer, 0, buffer.Length); //sends bytes to server
                    //}
                }
            }
        }
        catch (Exception ex)
        {
            return new SqlString(string.Format("#ERROR:{0}", ex.Message));
        }
    }

    #endregion
}
