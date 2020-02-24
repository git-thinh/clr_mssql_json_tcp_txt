using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Linq;
using System.Collections.Generic;
using Newtonsoft.Json;

public partial class StoredProcedures
{
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void ___changed_json(out SqlBoolean ok, out SqlString message)
    {
        string msg = "";
        bool ok_ = true;

        try
        {
            using (SqlConnection conn = new SqlConnection("context connection=true"))
            {
                conn.Open();
                // #___CHANGED(ID BIGINT, CACHE VARCHAR(255), DB_ACTION VARCHAR(255), SQL_CMD NVARCHAR(MAX), SORT INT)
                using (SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM #___CHANGED ORDER BY SORT", conn))
                {
                    DataTable dt = new DataTable();
                    adapter.Fill(dt);

                    if (dt.Rows.Count > 0)
                    {
                        foreach (DataRow r in dt.Rows)
                        {
                            try
                            {
                                SqlCommand cmd = new SqlCommand(@"
                                    IF @sort IS NULL SET @sort = -1;
                                    SELECT * FROM json___changed(@sort, @id, @cache, @db_action, @sql_cmd)", conn);
                                cmd.Parameters.AddWithValue("@sort", r["SORT"]);
                                cmd.Parameters.AddWithValue("@id", r["ID"]);
                                cmd.Parameters.AddWithValue("@cache", r["CACHE"]);
                                cmd.Parameters.AddWithValue("@db_action", r["DB_ACTION"]);
                                cmd.Parameters.AddWithValue("@sql_cmd", r["SQL_CMD"]);

                                SqlDataReader reader = cmd.ExecuteReader();
                                SqlContext.Pipe.Send(reader);
                            }
                            catch (Exception e1)
                            {
                                ok_ = false;
                                msg = "ERROR_EXE_JSON___CHANGED: " + e1.Message + Environment.NewLine + r["SQL_CMD"].ToString();
                                ok = new SqlBoolean(ok_);
                                message = new SqlString(msg);
                                return;
                            }
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            ok_ = false;
            msg = "ERROR_EXE: " + ex.Message;
        }

        ok = new SqlBoolean(ok_);
        message = new SqlString(msg);
    }


    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void execute_json(String query, String json, out SqlBoolean ok, out SqlString message)
    {
        string msg = "";
        bool ok_ = true;

        if (string.IsNullOrWhiteSpace(query))
        {
            msg = "";
            ok_ = false;
        }
        else
        {
            Dictionary<string, object> vals = null;
            if (!string.IsNullOrWhiteSpace(json))
            {
                try
                {
                    vals = JsonConvert.DeserializeObject<Dictionary<string, object>>(json);
                }
                catch (Exception ex)
                {
                    ok_ = false;
                    msg = "ERROR_JSON: " + ex.Message;
                }
            }

            if (ok_)
            {
                try
                {
                    using (SqlConnection conn = new SqlConnection("context connection=true"))
                    {
                        conn.Open();
                        SqlCommand cmd = new SqlCommand(query, conn);

                        foreach (var kv in vals)
                            cmd.Parameters.Add(new SqlParameter(kv.Key, kv.Value));

                        SqlDataReader reader = cmd.ExecuteReader();
                        SqlContext.Pipe.Send(reader);
                    }
                }
                catch (Exception ex)
                {
                    ok_ = false;
                    msg = "ERROR_EXE: " + ex.Message;
                }
            }
        }

        ok = new SqlBoolean(ok_);
        message = new SqlString(msg);
    }
}
