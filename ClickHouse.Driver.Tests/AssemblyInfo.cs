// Needed due to issues with floating point comparisons arising on ARM architectures
using System.Reflection;
[assembly:DefaultFloatingPointTolerance(1e-16)]
[assembly:AssemblyKeyFile("../sgKey.snk")]
