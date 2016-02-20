module influxdb.measurement;

import std.conv, std.string;
import std.algorithm : map, joiner, sort;
import std.traits;
import std.datetime;

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
        return data.keys.sort!().map!(key => key.replace(" ",
                "\\ ") ~ "=" ~ data[key]).joiner(",").to!string;
    }

    void opIndexAssign(T)(T value, string name)
    {
        data[name] = value.to!string;
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
    InfluxData id;
    alias id this;

    void opIndexAssign(bool value, string name)
    {
        id[name] = (value == true ? "True" : "False");
    }

    //Integer types need an additional i suffix
    void opIndexAssign(T)(T value, string name) if (isIntegral!T)
    {
        id[name] = value.to!string ~ "i";
    }

    void opIndexAssign(T)(T value, string name) if (!isIntegral!T)
    {
        data[name] = value.to!string;
    }

}

unittest
{
    auto v = InfluxValues();
    v["myInt"] = 4;
    v["myFloat"] = 0.2;
    v["myBool"] = true;
    string expected = "myBool=True,myFloat=0.2,myInt=4i";
    assert(v.toInflux() == expected, format("Error %s should be %s", v.toInflux(), expected));
}


import helpers : toUnixNs;
//creates a measurement
// Measurement m = Measurement("mySeries");
// m["aBool"] = true;
// m.addTag("tagKey", "tagVal");
struct Measurement
{

    private
    {
        string series;
        long timestamp;
        InfluxData tags;

    }
    InfluxValues values;
    //for now directly export the values
    alias values this;

    this(string name)
    {
        series = name;
    }

    this(string name, SysTime ts)
    {
        series = name;
        setTimestamp(ts);
    }
    
    void setTimestamp(long ts)
    {
        timestamp = ts;
    }

    
    void setTimestamp(SysTime t)
    {
        
        setTimestamp( t.toUnixNs );
    }

    //adds a tag and its value;
    void addTag(string key, string val)
    {
        tags[key] = val;
    }

    string toInfluxdbLine()
    {
        //do not send timestamp if its not set
        string ts = (timestamp > 0) ? timestamp.to!string : "";
        //do not add comma if no tags are being sent
        string sep = (tags.length > 0) ? "," : "";
        return format("%s%s%s %s %s", series, sep, tags.toInflux(), values.toInflux(), ts);
    }
}
