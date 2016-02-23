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
        import std.stdio;

        getConnection().write(dta);
    }
}

unittest
{

    //needs an influxdb running
    InfluxClient c = new InfluxClient();

    auto m = Measurement("xxx2");
    m["val1"] = 44L;
    //m.addTag("tagKey", "tagVal");
    auto m2 = Measurement("xxx2");
    m2["val1"] = 55L;
    bool b = true;
    m2["bools"] = b;
    m2["bools2"] = false;
    m2.addTag("tagKey", "tagVal2");
    c.sendMeasurement(m);
    c.sendMeasurement(m2);
}
