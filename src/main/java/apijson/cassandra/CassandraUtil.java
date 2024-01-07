/*Copyright ©2024 APIJSON(https://github.com/APIJSON)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

package apijson.cassandra;

import apijson.JSON;
import apijson.JSONResponse;
import apijson.NotNull;
import apijson.RequestMethod;
import apijson.orm.SQLConfig;
import com.alibaba.fastjson.JSONObject;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static apijson.orm.AbstractSQLExecutor.KEY_RAW_LIST;


/**
 * @author Lemon
 * @see DemoSQLExecutor 重写 execute 方法：
 *     \@Override
 *      public JSONObject execute(@NotNull SQLConfig<Long> config, boolean unknownType) throws Exception {
 *          if (config.isMilvus()) {
 *              return CassandraUtil.execute(config, null, unknownType);
 *          }
 *
 *          return super.execute(config, unknownType);
 *     }
 */
public class CassandraUtil {
    public static final String TAG = "CassandraUtil";

    public static <T> String getClientKey(@NotNull SQLConfig<T> config) {
        String uri = config.getDBUri();
        return uri + (uri.contains("?") ? "&" : "?") + "username=" + config.getDBAccount();
    }

    public static final Map<String, CqlSession> CLIENT_MAP = new LinkedHashMap<>();
    public static <T> CqlSession getSession(@NotNull SQLConfig<T> config) throws MalformedURLException {
        return getSession(config, true);
    }
    public static <T> CqlSession getSession(@NotNull SQLConfig<T> config, boolean autoNew) throws MalformedURLException {
        String key = getClientKey(config);

        CqlSession session = CLIENT_MAP.get(key);
        if (autoNew && session == null) {
            session = CqlSession.builder()
//                        .withCloudSecureConnectBundle(Paths.get("/path/to/secure-connect-database_name.zip"))
                    .withCloudSecureConnectBundle(new URL(config.getDBUri()))
//                    .withAuthCredentials(config.getDBAccount(), config.getDBPassword())
                    .withKeyspace(config.getSchema())
                    .build();

            CLIENT_MAP.put(key, session);
        }
        return session;
    }

    public static <T> void closeSession(@NotNull SQLConfig<T> config) throws MalformedURLException {
        CqlSession session = getSession(config, false);
        if (session != null) {
            String key = getClientKey(config);
            CLIENT_MAP.remove(key);

//            try {
                session.close();
//            }
//            catch (Throwable e) {
//                e.printStackTrace();
//            }
        }
    }

    public static <T> void closeAllSession() {
        Collection<CqlSession> cs = CLIENT_MAP.values();
        for (CqlSession c : cs) {
            try {
                c.close();
            }
            catch (Throwable e) {
                e.printStackTrace();
            }
        }
        
        CLIENT_MAP.clear();
    }


    public static <T> JSONObject execute(@NotNull SQLConfig<T> config, String sql, boolean unknownType) throws Exception {
        if (RequestMethod.isQueryMethod(config.getMethod())) {
            return execQuery(config, sql, unknownType);
        }

        return executeUpdate(config, sql);
    }

    public static <T> int execUpdate(SQLConfig<T> config, String sql) throws Exception {
        JSONObject result = executeUpdate(config, sql);
        return result.getIntValue(JSONResponse.KEY_COUNT);
    }

    public static <T> JSONObject executeUpdate(SQLConfig<T> config, String sql) throws Exception {
        return executeUpdate(null, config, sql);
    }
    public static <T> JSONObject executeUpdate(CqlSession session, SQLConfig<T> config, String sql) throws Exception {
        if (session == null) {
            session = getSession(config);
        }

        if (session == null) {
            session = getSession(config);
        }

        ResultSet rs = session.execute(sql);

        Row row = rs.one();
        if (row == null) {
            return null;
        }

        JSONObject result = new JSONObject(true);
        result.put(JSONResponse.KEY_COUNT, row.get(JSONResponse.KEY_COUNT, Integer.class));
        if (config.getId() != null) {
            result.put(JSONResponse.KEY_ID, config.getId());
        }

        return result;
    }


    public static <T> JSONObject execQuery(@NotNull SQLConfig<T> config, String sql, boolean unknownType) throws Exception {
        List<JSONObject> list = executeQuery(config, sql, unknownType);
        JSONObject result = list == null || list.isEmpty() ? null : list.get(0);
        if (result == null) {
            result = new JSONObject(true);
        }

        if (list != null && list.size() > 1) {
            result.put(KEY_RAW_LIST, list);
        }

        return result;
    }

    public static <T> List<JSONObject> executeQuery(@NotNull SQLConfig<T> config, String sql, boolean unknownType) throws Exception {
        return executeQuery(null, config, sql, unknownType);
    }
    public static <T> List<JSONObject> executeQuery(CqlSession session, @NotNull SQLConfig<T> config, String sql, boolean unknownType) throws Exception {
        if (session == null) {
            session = getSession(config);
        }

        //            if (config.isPrepared()) {
        //                PreparedStatement stt = session.prepare(sql);
        //
        //                List<Object> pl = config.getPreparedValueList();
        //                if (pl != null) {
        //                    for (Object o : pl) {
        //                        stt.bind(pl.toArray());
        //                    }
        //                }
        //                sql = stt.getQuery();
        //            }

        ResultSet rs = session.execute(sql);

        List<Row> list = rs.all();
        if (list == null) {
            return null;
        }

        List<JSONObject> resultList = new ArrayList<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            resultList.add(JSON.parseObject(list.get(i)));
        }

        return resultList;
    }

}
