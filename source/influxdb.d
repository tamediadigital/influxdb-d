module influxdb;

import std.conv, std.string;
import std.algorithm : map, joiner, sort;

struct InfluxData
{
    string[string] data;
    alias data this;

    @property string toInflux()
    {
        if (data.length == 0)
        {
            return "";
        }
        return data.keys.sort!().map!(key => key.replace(" ", "\\ ") ~ "=" ~ data[key]).joiner(",").to!string;
    }
}

unittest
{

    InfluxData d = InfluxData();
    assert(d.toInflux == "");
    d["a"] = "1";
    d["b"] = "2";
    d["c c"] = "3";
    assert(d.toInflux == "a=1,b=2,c\\ c=3");

}

struct InfluxValues
{
    InfluxData data;
    alias data this;
}

struct Measurement
{

    private
    {
        string series;
        ulong timestamp;
        InfluxData tags;
        InfluxValues values;
    }

    this(string name)
    {
        series = name;
    }

    void opIndexAssign(string name, double value)
    {
        values[name] = value.to!string;
    }

    void opIndexAssign(string name, long value)
    {
        values[name] = format("%si", value);
    }

    string toInfluxdbLine()
    {
        string ret;

        ret = format("%s%s %s %s", series, values.toInflux(), tags.toInflux(), timestamp);
        return ret;
    }
}
