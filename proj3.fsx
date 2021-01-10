#r "nuget: Akka.FSharp" 

#if INTERACTIVE
#load "actors.fs"
#endif

open pastry

if fsi.CommandLineArgs.Length < 3 then
    printfn "not enough arguments found"
else
    Pastry.Start (int fsi.CommandLineArgs.[1],int fsi.CommandLineArgs.[2])