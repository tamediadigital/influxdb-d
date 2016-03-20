module influxdb.client;

import vibe.core.net;
import vibe.core.log;

string DEFAULT_ADDRESS = "127.0.0.1";
ushort DEFAULT_UDP_PORT = 4455;

private final class InfluxConnection
{
    private
    {
        string m_host;
        ushort m_port;
        UDPConnection m_conn;
    }

    this(string host, ushort port)
    {
        m_host = host;
        m_port = port;
        m_conn = listenUDP(0);

    }

    InfluxConnection connect()
    {
        m_conn.connect(m_host, m_port);
        return this;
    }

    void write(string payload)
    {
        m_conn.send(cast(ubyte[]) payload);
    }

}

import vibe.core.connectionpool;
import influxdb.measurement;

final class InfluxClient
{
    private
    {
        ConnectionPool!InfluxConnection m_connections;
    }

    this(string host = DEFAULT_ADDRESS, ushort port = DEFAULT_UDP_PORT)
    {
        m_connections = new ConnectionPool!InfluxConnection({
            return new InfluxConnection(host, port).connect();
        });
    }

    LockedConnection!InfluxConnection getConnection()
    {
        return m_connections.lockConnection();
    }

    import influxdb.measurement;

    void sendMeasurement(Measurement m)
    {
        string dta = m.toInfluxdbLine();
        getConnection().write(dta);
    }

    void sendMeasurements(Measurement[] m...)
    {
        import std.algorithm : map;
        import std.array : array, join ;
        string dta = m.map!( point => point.toInfluxdbLine()).array().join("\n");
        getConnection().write(dta);
    }
}


import vibe.http.client;
import vibe.stream.operations;
import vibe.core.log;
final class InfluxHTTPClient
{
    private
    {
        string m_url;
        string m_db;
    }
    this(string url, string db)
    {
        m_url = url;
        m_db = db;
    }

    void sendMeasurement(Measurement m)
    {
       sendMeasurements([m]);
    }

    void sendMeasurements(Measurement[] m...)
    {
        import std.algorithm : map;
        import std.array : array, join ;
        string dta = m.map!( point => point.toInfluxdbLine()).array().join("\n");

        requestHTTP(m_url ~ "/write?db=" ~m_db ,
            (scope req){
                req.method = HTTPMethod.POST;
                req.writeBody(cast(ubyte[]) dta);
            },
            (scope res){
                if (res.statusCode != 204)
                {
                    logError("Failed to send data. return code: %s", res.statusCode);
                    throw new Exception(res.bodyReader.readAllUTF8());
                }
                res.dropBody();
            }
        );
    }
}

unittest
{

    //needs an influxdb running
    InfluxHTTPClient c = new InfluxHTTPClient("http://localhost:8086", "mydb");

    auto m = Measurement("xxx2");
    m["val1"] = 44L;
    //m.addTag("tagKey", "tagVal");
    auto m2 = Measurement("xxx2");
    m2["val1"] = 55L;
    bool b = true;
    m2["bools"] = b;
    m2["bools2"] = false;
    m2.addTag("tagKey", "tagVal2");
    c.sendMeasurements(m, m2);

}
