module helpers;

long toUnixNs(SysTime t)
{
  return (t.stdTime - 621_355_968_000_000_000L)*100;
}