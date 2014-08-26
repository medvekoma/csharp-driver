
//  Microsoft (R) .NET Framework IL Disassembler.  Version 4.0.30319.1
//  Copyright (c) Microsoft Corporation.  All rights reserved.



// Metadata version: v2.0.50727
.module extern kernel32
.module extern snappy32.dll
.module extern snappy64.dll
.assembly extern mscorlib
{
  .publickeytoken = (B7 7A 5C 56 19 34 E0 89 )                         // .z\V.4..
  .ver 2:0:0:0
}
.assembly extern System
{
  .publickeytoken = (B7 7A 5C 56 19 34 E0 89 )                         // .z\V.4..
  .ver 2:0:0:0
}
.assembly extern Crc32C.NET
{
  .ver 1:0:5:0
}
.assembly Snappy.NET
{
  .custom instance void [mscorlib]System.Reflection.AssemblyTitleAttribute::.ctor(string) = ( 01 00 0F 53 6E 61 70 70 79 20 66 6F 72 20 2E 4E   // ...Snappy for .N
                                                                                              45 54 00 00 )                                     // ET..
  .custom instance void [mscorlib]System.Reflection.AssemblyDescriptionAttribute::.ctor(string) = ( 01 00 80 C2 53 6E 61 70 70 79 20 69 73 20 61 6E   // ....Snappy is an
                                                                                                    20 65 78 74 72 65 6D 65 6C 79 20 66 61 73 74 20   //  extremely fast 
                                                                                                    63 6F 6D 70 72 65 73 73 6F 72 20 28 32 35 30 4D   // compressor (250M
                                                                                                    42 2F 73 29 20 61 6E 64 20 64 65 63 6F 6D 70 72   // B/s) and decompr
                                                                                                    65 73 73 6F 72 20 28 35 30 30 4D 42 2F 73 29 2E   // essor (500MB/s).
                                                                                                    20 53 6E 61 70 70 79 2E 4E 45 54 20 69 73 20 61   //  Snappy.NET is a
                                                                                                    20 50 2F 49 6E 76 6F 6B 65 20 77 72 61 70 70 65   //  P/Invoke wrappe
                                                                                                    72 20 61 72 6F 75 6E 64 20 6E 61 74 69 76 65 20   // r around native 
                                                                                                    53 6E 61 70 70 79 2C 20 77 68 69 63 68 20 61 64   // Snappy, which ad
                                                                                                    64 69 74 69 6F 6E 61 6C 6C 79 20 69 6D 70 6C 65   // ditionally imple
                                                                                                    6D 65 6E 74 73 20 53 6E 61 70 70 79 20 66 72 61   // ments Snappy fra
                                                                                                    6D 69 6E 67 2F 73 74 72 65 61 6D 69 6E 67 20 66   // ming/streaming f
                                                                                                    6F 72 6D 61 74 2E 00 00 )                         // ormat...
  .custom instance void [mscorlib]System.Reflection.AssemblyConfigurationAttribute::.ctor(string) = ( 01 00 00 00 00 ) 
  .custom instance void [mscorlib]System.Runtime.CompilerServices.RuntimeCompatibilityAttribute::.ctor() = ( 01 00 01 00 54 02 16 57 72 61 70 4E 6F 6E 45 78   // ....T..WrapNonEx
                                                                                                             63 65 70 74 69 6F 6E 54 68 72 6F 77 73 01 )       // ceptionThrows.
  .custom instance void [mscorlib]System.Reflection.AssemblyCompanyAttribute::.ctor(string) = ( 01 00 0D 52 6F 62 65 72 74 20 56 61 C5 BE 61 6E   // ...Robert Va..an
                                                                                                00 00 ) 
  .custom instance void [mscorlib]System.Reflection.AssemblyProductAttribute::.ctor(string) = ( 01 00 0F 53 6E 61 70 70 79 20 66 6F 72 20 2E 4E   // ...Snappy for .N
                                                                                                45 54 00 00 )                                     // ET..
  .custom instance void [mscorlib]System.Runtime.CompilerServices.CompilationRelaxationsAttribute::.ctor(int32) = ( 01 00 08 00 00 00 00 00 ) 
  .custom instance void [mscorlib]System.Reflection.AssemblyTrademarkAttribute::.ctor(string) = ( 01 00 00 00 00 ) 
  .custom instance void [mscorlib]System.Reflection.AssemblyCopyrightAttribute::.ctor(string) = ( 01 00 1F 43 6F 70 79 72 69 67 68 74 20 C2 A9 20   // ...Copyright .. 
                                                                                                  32 30 31 34 20 52 6F 62 65 72 74 20 56 61 C5 BE   // 2014 Robert Va..
                                                                                                  61 6E 00 00 )                                     // an..
  .custom instance void [mscorlib]System.Runtime.InteropServices.ComVisibleAttribute::.ctor(bool) = ( 01 00 00 00 00 ) 
  .custom instance void [mscorlib]System.Runtime.InteropServices.GuidAttribute::.ctor(string) = ( 01 00 24 31 39 31 65 65 64 37 62 2D 38 62 64 38   // ..$191eed7b-8bd8
                                                                                                  2D 34 31 62 37 2D 62 31 32 37 2D 38 66 34 36 65   // -41b7-b127-8f46e
                                                                                                  38 66 32 65 38 65 37 00 00 )                      // 8f2e8e7..

  // --- The following custom attribute is added automatically, do not uncomment -------
  //  .custom instance void [mscorlib]System.Diagnostics.DebuggableAttribute::.ctor(valuetype [mscorlib]System.Diagnostics.DebuggableAttribute/DebuggingModes) = ( 01 00 02 00 00 00 00 00 ) 

  .permissionset reqmin
             = {[mscorlib]System.Security.Permissions.SecurityPermissionAttribute = {property bool 'SkipVerification' = bool(true)}}
  .hash algorithm 0x00008004
  .ver 1:1:1:8
}
.mresource public Snappy.snappy32.dll
{
  // Offset: 0x00000000 Length: 0x00038A00
  // WARNING: managed resource file Snappy.snappy32.dll created
}
.mresource public Snappy.snappy64.dll
{
  // Offset: 0x00038A08 Length: 0x00042000
  // WARNING: managed resource file Snappy.snappy64.dll created
}
.module Snappy.NET.dll
// MVID: {5892DCEF-F5D7-4B2E-B51C-194EB7BAAF2A}
.custom instance void [mscorlib]System.Security.UnverifiableCodeAttribute::.ctor() = ( 01 00 00 00 ) 
.imagebase 0x10000000
.file alignment 0x00000200
.stackreserve 0x00100000
.subsystem 0x0003       // WINDOWS_CUI
.corflags 0x00000001    //  ILONLY
// Image base: 0x01FF0000


// =============== CLASS MEMBERS DECLARATION ===================

.class private abstract auto ansi beforefieldinit Snappy.NativeProxy
       extends [mscorlib]System.Object
{
  .field public static initonly class Snappy.NativeProxy Instance
  .method family hidebysig specialname rtspecialname 
          instance void  .ctor(string name) cil managed
  {
    // Code size       277 (0x115)
    .maxstack  4
    .locals init (class [mscorlib]System.Reflection.Assembly V_0,
             string V_1,
             string V_2,
             uint8[] V_3,
             class [mscorlib]System.IO.Stream V_4,
             class [mscorlib]System.IO.MemoryStream V_5,
             uint8[] V_6,
             int32 V_7,
             class [mscorlib]System.IO.FileStream V_8,
             native int V_9)
    IL_0000:  ldarg.0
    IL_0001:  call       instance void [mscorlib]System.Object::.ctor()
    IL_0006:  call       class [mscorlib]System.Reflection.Assembly [mscorlib]System.Reflection.Assembly::GetExecutingAssembly()
    IL_000b:  stloc.0
    IL_000c:  call       string [mscorlib]System.IO.Path::GetTempPath()
    IL_0011:  ldstr      "Snappy.NET-"
    IL_0016:  ldloc.0
    IL_0017:  callvirt   instance class [mscorlib]System.Reflection.AssemblyName [mscorlib]System.Reflection.Assembly::GetName()
    IL_001c:  callvirt   instance class [mscorlib]System.Version [mscorlib]System.Reflection.AssemblyName::get_Version()
    IL_0021:  callvirt   instance string [mscorlib]System.Object::ToString()
    IL_0026:  call       string [mscorlib]System.String::Concat(string,
                                                                string)
    IL_002b:  call       string [mscorlib]System.IO.Path::Combine(string,
                                                                  string)
    IL_0030:  stloc.1
    IL_0031:  ldloc.1
    IL_0032:  call       class [mscorlib]System.IO.DirectoryInfo [mscorlib]System.IO.Directory::CreateDirectory(string)
    IL_0037:  pop
    IL_0038:  ldloc.1
    IL_0039:  ldarg.1
    IL_003a:  call       string [mscorlib]System.IO.Path::Combine(string,
                                                                  string)
    IL_003f:  stloc.2
    IL_0040:  ldloc.0
    IL_0041:  ldstr      "Snappy."
    IL_0046:  ldarg.1
    IL_0047:  call       string [mscorlib]System.String::Concat(string,
                                                                string)
    IL_004c:  callvirt   instance class [mscorlib]System.IO.Stream [mscorlib]System.Reflection.Assembly::GetManifestResourceStream(string)
    IL_0051:  stloc.s    V_4
    .try
    {
      IL_0053:  newobj     instance void [mscorlib]System.IO.MemoryStream::.ctor()
      IL_0058:  stloc.s    V_5
      .try
      {
        IL_005a:  ldc.i4     0x1000
        IL_005f:  newarr     [mscorlib]System.Byte
        IL_0064:  stloc.s    V_6
        IL_0066:  br.s       IL_0074

        IL_0068:  ldloc.s    V_5
        IL_006a:  ldloc.s    V_6
        IL_006c:  ldc.i4.0
        IL_006d:  ldloc.s    V_7
        IL_006f:  callvirt   instance void [mscorlib]System.IO.Stream::Write(uint8[],
                                                                             int32,
                                                                             int32)
        IL_0074:  ldloc.s    V_4
        IL_0076:  ldloc.s    V_6
        IL_0078:  ldc.i4.0
        IL_0079:  ldloc.s    V_6
        IL_007b:  ldlen
        IL_007c:  conv.i4
        IL_007d:  callvirt   instance int32 [mscorlib]System.IO.Stream::Read(uint8[],
                                                                             int32,
                                                                             int32)
        IL_0082:  dup
        IL_0083:  stloc.s    V_7
        IL_0085:  brtrue.s   IL_0068

        IL_0087:  ldloc.s    V_5
        IL_0089:  callvirt   instance void [mscorlib]System.IO.Stream::Close()
        IL_008e:  ldloc.s    V_5
        IL_0090:  callvirt   instance uint8[] [mscorlib]System.IO.MemoryStream::ToArray()
        IL_0095:  stloc.3
        IL_0096:  leave.s    IL_00a4

      }  // end .try
      finally
      {
        IL_0098:  ldloc.s    V_5
        IL_009a:  brfalse.s  IL_00a3

        IL_009c:  ldloc.s    V_5
        IL_009e:  callvirt   instance void [mscorlib]System.IDisposable::Dispose()
        IL_00a3:  endfinally
      }  // end handler
      IL_00a4:  leave.s    IL_00b2

    }  // end .try
    finally
    {
      IL_00a6:  ldloc.s    V_4
      IL_00a8:  brfalse.s  IL_00b1

      IL_00aa:  ldloc.s    V_4
      IL_00ac:  callvirt   instance void [mscorlib]System.IDisposable::Dispose()
      IL_00b1:  endfinally
    }  // end handler
    IL_00b2:  ldloc.2
    IL_00b3:  call       bool [mscorlib]System.IO.File::Exists(string)
    IL_00b8:  brfalse.s  IL_00c8

    IL_00ba:  ldloc.2
    IL_00bb:  call       uint8[] [mscorlib]System.IO.File::ReadAllBytes(string)
    IL_00c0:  ldloc.3
    IL_00c1:  call       bool Snappy.Utils::BuffersEqual(uint8[],
                                                         uint8[])
    IL_00c6:  brtrue.s   IL_00ed

    IL_00c8:  ldloc.2
    IL_00c9:  ldc.i4.2
    IL_00ca:  ldc.i4.2
    IL_00cb:  ldc.i4.0
    IL_00cc:  call       class [mscorlib]System.IO.FileStream [mscorlib]System.IO.File::Open(string,
                                                                                             valuetype [mscorlib]System.IO.FileMode,
                                                                                             valuetype [mscorlib]System.IO.FileAccess,
                                                                                             valuetype [mscorlib]System.IO.FileShare)
    IL_00d1:  stloc.s    V_8
    .try
    {
      IL_00d3:  ldloc.s    V_8
      IL_00d5:  ldloc.3
      IL_00d6:  ldc.i4.0
      IL_00d7:  ldloc.3
      IL_00d8:  ldlen
      IL_00d9:  conv.i4
      IL_00da:  callvirt   instance void [mscorlib]System.IO.Stream::Write(uint8[],
                                                                           int32,
                                                                           int32)
      IL_00df:  leave.s    IL_00ed

    }  // end .try
    finally
    {
      IL_00e1:  ldloc.s    V_8
      IL_00e3:  brfalse.s  IL_00ec

      IL_00e5:  ldloc.s    V_8
      IL_00e7:  callvirt   instance void [mscorlib]System.IDisposable::Dispose()
      IL_00ec:  endfinally
    }  // end handler
    IL_00ed:  ldloc.2
    IL_00ee:  call       native int Snappy.NativeProxy::LoadLibrary(string)
    IL_00f3:  stloc.s    V_9
    IL_00f5:  ldloc.s    V_9
    IL_00f7:  ldsfld     native int [mscorlib]System.IntPtr::Zero
    IL_00fc:  call       bool [mscorlib]System.IntPtr::op_Equality(native int,
                                                                   native int)
    IL_0101:  brfalse.s  IL_0114

    IL_0103:  ldstr      "Cannot load "
    IL_0108:  ldarg.1
    IL_0109:  call       string [mscorlib]System.String::Concat(string,
                                                                string)
    IL_010e:  newobj     instance void [mscorlib]System.ApplicationException::.ctor(string)
    IL_0113:  throw

    IL_0114:  ret
  } // end of method NativeProxy::.ctor

  .method public hidebysig newslot abstract virtual 
          instance valuetype Snappy.SnappyStatus 
          Compress(uint8* input,
                   int32 inLength,
                   uint8* output,
                   int32& outLength) cil managed
  {
  } // end of method NativeProxy::Compress

  .method public hidebysig newslot abstract virtual 
          instance valuetype Snappy.SnappyStatus 
          Uncompress(uint8* input,
                     int32 inLength,
                     uint8* output,
                     int32& outLength) cil managed
  {
  } // end of method NativeProxy::Uncompress

  .method public hidebysig newslot abstract virtual 
          instance int32  GetMaxCompressedLength(int32 inLength) cil managed
  {
  } // end of method NativeProxy::GetMaxCompressedLength

  .method public hidebysig newslot abstract virtual 
          instance valuetype Snappy.SnappyStatus 
          GetUncompressedLength(uint8* input,
                                int32 inLength,
                                [out] int32& outLength) cil managed
  {
  } // end of method NativeProxy::GetUncompressedLength

  .method public hidebysig newslot abstract virtual 
          instance valuetype Snappy.SnappyStatus 
          ValidateCompressedBuffer(uint8* input,
                                   int32 inLength) cil managed
  {
  } // end of method NativeProxy::ValidateCompressedBuffer

  .method private hidebysig static pinvokeimpl("kernel32" unicode lasterr winapi) 
          native int  LoadLibrary(string lpFileName) cil managed preservesig
  {
  }
  .method private hidebysig specialname rtspecialname static 
          void  .cctor() cil managed
  {
    // Code size       26 (0x1a)
    .maxstack  8
    IL_0000:  call       int32 [mscorlib]System.IntPtr::get_Size()
    IL_0005:  ldc.i4.4
    IL_0006:  beq.s      IL_000f

    IL_0008:  newobj     instance void Snappy.Native64::.ctor()
    IL_000d:  br.s       IL_0014

    IL_000f:  newobj     instance void Snappy.Native32::.ctor()
    IL_0014:  stsfld     class Snappy.NativeProxy Snappy.NativeProxy::Instance
    IL_0019:  ret
  } // end of method NativeProxy::.cctor

} // end of class Snappy.NativeProxy

.class private auto ansi beforefieldinit Snappy.Native32
       extends Snappy.NativeProxy
{
  .method public hidebysig specialname rtspecialname 
          instance void  .ctor() cil managed
  {
    // Code size       12 (0xc)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldstr      "snappy32.dll"
    IL_0006:  call       instance void Snappy.NativeProxy::.ctor(string)
    IL_000b:  ret
  } // end of method Native32::.ctor

  .method public hidebysig virtual instance valuetype Snappy.SnappyStatus 
          Compress(uint8* input,
                   int32 inLength,
                   uint8* output,
                   int32& outLength) cil managed
  {
    // Code size       24 (0x18)
    .maxstack  4
    .locals init (uint32 V_0,
             valuetype Snappy.SnappyStatus V_1)
    IL_0000:  ldarg.s    outLength
    IL_0002:  ldind.i4
    IL_0003:  conv.ovf.u4
    IL_0004:  stloc.0
    IL_0005:  ldarg.1
    IL_0006:  ldarg.2
    IL_0007:  conv.ovf.u4
    IL_0008:  ldarg.3
    IL_0009:  ldloca.s   V_0
    IL_000b:  call       valuetype Snappy.SnappyStatus Snappy.Native32::snappy_compress(uint8*,
                                                                                        uint32,
                                                                                        uint8*,
                                                                                        uint32&)
    IL_0010:  stloc.1
    IL_0011:  ldarg.s    outLength
    IL_0013:  ldloc.0
    IL_0014:  conv.ovf.i4.un
    IL_0015:  stind.i4
    IL_0016:  ldloc.1
    IL_0017:  ret
  } // end of method Native32::Compress

  .method public hidebysig virtual instance valuetype Snappy.SnappyStatus 
          Uncompress(uint8* input,
                     int32 inLength,
                     uint8* output,
                     int32& outLength) cil managed
  {
    // Code size       24 (0x18)
    .maxstack  4
    .locals init (uint32 V_0,
             valuetype Snappy.SnappyStatus V_1)
    IL_0000:  ldarg.s    outLength
    IL_0002:  ldind.i4
    IL_0003:  conv.ovf.u4
    IL_0004:  stloc.0
    IL_0005:  ldarg.1
    IL_0006:  ldarg.2
    IL_0007:  conv.ovf.u4
    IL_0008:  ldarg.3
    IL_0009:  ldloca.s   V_0
    IL_000b:  call       valuetype Snappy.SnappyStatus Snappy.Native32::snappy_uncompress(uint8*,
                                                                                          uint32,
                                                                                          uint8*,
                                                                                          uint32&)
    IL_0010:  stloc.1
    IL_0011:  ldarg.s    outLength
    IL_0013:  ldloc.0
    IL_0014:  conv.ovf.i4.un
    IL_0015:  stind.i4
    IL_0016:  ldloc.1
    IL_0017:  ret
  } // end of method Native32::Uncompress

  .method public hidebysig virtual instance int32 
          GetMaxCompressedLength(int32 inLength) cil managed
  {
    // Code size       9 (0x9)
    .maxstack  8
    IL_0000:  ldarg.1
    IL_0001:  conv.ovf.u4
    IL_0002:  call       uint32 Snappy.Native32::snappy_max_compressed_length(uint32)
    IL_0007:  conv.ovf.i4.un
    IL_0008:  ret
  } // end of method Native32::GetMaxCompressedLength

  .method public hidebysig virtual instance valuetype Snappy.SnappyStatus 
          GetUncompressedLength(uint8* input,
                                int32 inLength,
                                [out] int32& outLength) cil managed
  {
    // Code size       17 (0x11)
    .maxstack  3
    .locals init (uint32 V_0,
             valuetype Snappy.SnappyStatus V_1)
    IL_0000:  ldarg.1
    IL_0001:  ldarg.2
    IL_0002:  conv.ovf.u4
    IL_0003:  ldloca.s   V_0
    IL_0005:  call       valuetype Snappy.SnappyStatus Snappy.Native32::snappy_uncompressed_length(uint8*,
                                                                                                   uint32,
                                                                                                   uint32&)
    IL_000a:  stloc.1
    IL_000b:  ldarg.3
    IL_000c:  ldloc.0
    IL_000d:  conv.ovf.i4.un
    IL_000e:  stind.i4
    IL_000f:  ldloc.1
    IL_0010:  ret
  } // end of method Native32::GetUncompressedLength

  .method public hidebysig virtual instance valuetype Snappy.SnappyStatus 
          ValidateCompressedBuffer(uint8* input,
                                   int32 inLength) cil managed
  {
    // Code size       9 (0x9)
    .maxstack  8
    IL_0000:  ldarg.1
    IL_0001:  ldarg.2
    IL_0002:  conv.ovf.u4
    IL_0003:  call       valuetype Snappy.SnappyStatus Snappy.Native32::snappy_validate_compressed_buffer(uint8*,
                                                                                                          uint32)
    IL_0008:  ret
  } // end of method Native32::ValidateCompressedBuffer

  .method private hidebysig static pinvokeimpl("snappy32.dll" cdecl) 
          valuetype Snappy.SnappyStatus  snappy_compress(uint8* input,
                                                         uint32 input_length,
                                                         uint8* output,
                                                         uint32& output_length) cil managed preservesig
  {
  }
  .method private hidebysig static pinvokeimpl("snappy32.dll" cdecl) 
          valuetype Snappy.SnappyStatus  snappy_uncompress(uint8* input,
                                                           uint32 input_length,
                                                           uint8* output,
                                                           uint32& output_length) cil managed preservesig
  {
  }
  .method private hidebysig static pinvokeimpl("snappy32.dll" cdecl) 
          uint32  snappy_max_compressed_length(uint32 input_length) cil managed preservesig
  {
  }
  .method private hidebysig static pinvokeimpl("snappy32.dll" cdecl) 
          valuetype Snappy.SnappyStatus  snappy_uncompressed_length(uint8* input,
                                                                    uint32 input_length,
                                                                    [out] uint32& output_length) cil managed preservesig
  {
  }
  .method private hidebysig static pinvokeimpl("snappy32.dll" cdecl) 
          valuetype Snappy.SnappyStatus  snappy_validate_compressed_buffer(uint8* input,
                                                                           uint32 input_length) cil managed preservesig
  {
  }
} // end of class Snappy.Native32

.class private auto ansi beforefieldinit Snappy.Native64
       extends Snappy.NativeProxy
{
  .method public hidebysig specialname rtspecialname 
          instance void  .ctor() cil managed
  {
    // Code size       12 (0xc)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldstr      "snappy64.dll"
    IL_0006:  call       instance void Snappy.NativeProxy::.ctor(string)
    IL_000b:  ret
  } // end of method Native64::.ctor

  .method public hidebysig virtual instance valuetype Snappy.SnappyStatus 
          Compress(uint8* input,
                   int32 inLength,
                   uint8* output,
                   int32& outLength) cil managed
  {
    // Code size       24 (0x18)
    .maxstack  4
    .locals init (uint64 V_0,
             valuetype Snappy.SnappyStatus V_1)
    IL_0000:  ldarg.s    outLength
    IL_0002:  ldind.i4
    IL_0003:  conv.ovf.u8
    IL_0004:  stloc.0
    IL_0005:  ldarg.1
    IL_0006:  ldarg.2
    IL_0007:  conv.ovf.u8
    IL_0008:  ldarg.3
    IL_0009:  ldloca.s   V_0
    IL_000b:  call       valuetype Snappy.SnappyStatus Snappy.Native64::snappy_compress(uint8*,
                                                                                        uint64,
                                                                                        uint8*,
                                                                                        uint64&)
    IL_0010:  stloc.1
    IL_0011:  ldarg.s    outLength
    IL_0013:  ldloc.0
    IL_0014:  conv.ovf.i4.un
    IL_0015:  stind.i4
    IL_0016:  ldloc.1
    IL_0017:  ret
  } // end of method Native64::Compress

  .method public hidebysig virtual instance valuetype Snappy.SnappyStatus 
          Uncompress(uint8* input,
                     int32 inLength,
                     uint8* output,
                     int32& outLength) cil managed
  {
    // Code size       24 (0x18)
    .maxstack  4
    .locals init (uint64 V_0,
             valuetype Snappy.SnappyStatus V_1)
    IL_0000:  ldarg.s    outLength
    IL_0002:  ldind.i4
    IL_0003:  conv.ovf.u8
    IL_0004:  stloc.0
    IL_0005:  ldarg.1
    IL_0006:  ldarg.2
    IL_0007:  conv.ovf.u8
    IL_0008:  ldarg.3
    IL_0009:  ldloca.s   V_0
    IL_000b:  call       valuetype Snappy.SnappyStatus Snappy.Native64::snappy_uncompress(uint8*,
                                                                                          uint64,
                                                                                          uint8*,
                                                                                          uint64&)
    IL_0010:  stloc.1
    IL_0011:  ldarg.s    outLength
    IL_0013:  ldloc.0
    IL_0014:  conv.ovf.i4.un
    IL_0015:  stind.i4
    IL_0016:  ldloc.1
    IL_0017:  ret
  } // end of method Native64::Uncompress

  .method public hidebysig virtual instance int32 
          GetMaxCompressedLength(int32 inLength) cil managed
  {
    // Code size       9 (0x9)
    .maxstack  8
    IL_0000:  ldarg.1
    IL_0001:  conv.ovf.u8
    IL_0002:  call       uint64 Snappy.Native64::snappy_max_compressed_length(uint64)
    IL_0007:  conv.ovf.i4.un
    IL_0008:  ret
  } // end of method Native64::GetMaxCompressedLength

  .method public hidebysig virtual instance valuetype Snappy.SnappyStatus 
          GetUncompressedLength(uint8* input,
                                int32 inLength,
                                [out] int32& outLength) cil managed
  {
    // Code size       17 (0x11)
    .maxstack  3
    .locals init (uint64 V_0,
             valuetype Snappy.SnappyStatus V_1)
    IL_0000:  ldarg.1
    IL_0001:  ldarg.2
    IL_0002:  conv.ovf.u8
    IL_0003:  ldloca.s   V_0
    IL_0005:  call       valuetype Snappy.SnappyStatus Snappy.Native64::snappy_uncompressed_length(uint8*,
                                                                                                   uint64,
                                                                                                   uint64&)
    IL_000a:  stloc.1
    IL_000b:  ldarg.3
    IL_000c:  ldloc.0
    IL_000d:  conv.ovf.i4.un
    IL_000e:  stind.i4
    IL_000f:  ldloc.1
    IL_0010:  ret
  } // end of method Native64::GetUncompressedLength

  .method public hidebysig virtual instance valuetype Snappy.SnappyStatus 
          ValidateCompressedBuffer(uint8* input,
                                   int32 inLength) cil managed
  {
    // Code size       9 (0x9)
    .maxstack  8
    IL_0000:  ldarg.1
    IL_0001:  ldarg.2
    IL_0002:  conv.ovf.u8
    IL_0003:  call       valuetype Snappy.SnappyStatus Snappy.Native64::snappy_validate_compressed_buffer(uint8*,
                                                                                                          uint64)
    IL_0008:  ret
  } // end of method Native64::ValidateCompressedBuffer

  .method private hidebysig static pinvokeimpl("snappy64.dll" cdecl) 
          valuetype Snappy.SnappyStatus  snappy_compress(uint8* input,
                                                         uint64 input_length,
                                                         uint8* output,
                                                         uint64& output_length) cil managed preservesig
  {
  }
  .method private hidebysig static pinvokeimpl("snappy64.dll" cdecl) 
          valuetype Snappy.SnappyStatus  snappy_uncompress(uint8* input,
                                                           uint64 input_length,
                                                           uint8* output,
                                                           uint64& output_length) cil managed preservesig
  {
  }
  .method private hidebysig static pinvokeimpl("snappy64.dll" cdecl) 
          uint64  snappy_max_compressed_length(uint64 input_length) cil managed preservesig
  {
  }
  .method private hidebysig static pinvokeimpl("snappy64.dll" cdecl) 
          valuetype Snappy.SnappyStatus  snappy_uncompressed_length(uint8* input,
                                                                    uint64 input_length,
                                                                    [out] uint64& output_length) cil managed preservesig
  {
  }
  .method private hidebysig static pinvokeimpl("snappy64.dll" cdecl) 
          valuetype Snappy.SnappyStatus  snappy_validate_compressed_buffer(uint8* input,
                                                                           uint64 input_length) cil managed preservesig
  {
  }
} // end of class Snappy.Native64

.class public abstract auto ansi sealed beforefieldinit Snappy.SnappyCodec
       extends [mscorlib]System.Object
{
  .method public hidebysig static int32  Compress(uint8[] input,
                                                  int32 offset,
                                                  int32 length,
                                                  uint8[] output,
                                                  int32 outOffset) cil managed
  {
    // Code size       158 (0x9e)
    .maxstack  5
    .locals init (int32 V_0,
             uint8& pinned V_1,
             uint8& pinned V_2,
             valuetype Snappy.SnappyStatus V_3,
             int32 V_4)
    IL_0000:  ldarg.0
    IL_0001:  brfalse.s  IL_0006

    IL_0003:  ldarg.3
    IL_0004:  brtrue.s   IL_000c

    IL_0006:  newobj     instance void [mscorlib]System.ArgumentNullException::.ctor()
    IL_000b:  throw

    IL_000c:  ldarg.1
    IL_000d:  ldc.i4.0
    IL_000e:  blt.s      IL_001c

    IL_0010:  ldarg.2
    IL_0011:  ldc.i4.0
    IL_0012:  blt.s      IL_001c

    IL_0014:  ldarg.1
    IL_0015:  ldarg.2
    IL_0016:  add
    IL_0017:  ldarg.0
    IL_0018:  ldlen
    IL_0019:  conv.i4
    IL_001a:  ble.s      IL_0027

    IL_001c:  ldstr      "Selected range is outside the bounds of the input "
    + "array"
    IL_0021:  newobj     instance void [mscorlib]System.ArgumentOutOfRangeException::.ctor(string)
    IL_0026:  throw

    IL_0027:  ldarg.s    outOffset
    IL_0029:  ldc.i4.0
    IL_002a:  blt.s      IL_0033

    IL_002c:  ldarg.s    outOffset
    IL_002e:  ldarg.3
    IL_002f:  ldlen
    IL_0030:  conv.i4
    IL_0031:  blt.s      IL_003e

    IL_0033:  ldstr      "Output offset is outside the bounds of the output "
    + "array"
    IL_0038:  newobj     instance void [mscorlib]System.ArgumentOutOfRangeException::.ctor(string)
    IL_003d:  throw

    IL_003e:  ldarg.3
    IL_003f:  ldlen
    IL_0040:  conv.i4
    IL_0041:  ldarg.s    outOffset
    IL_0043:  sub
    IL_0044:  stloc.0
    IL_0045:  ldarg.1
    IL_0046:  ldarg.0
    IL_0047:  ldlen
    IL_0048:  conv.i4
    IL_0049:  bne.un.s   IL_0056

    IL_004b:  ldc.i4.1
    IL_004c:  newarr     [mscorlib]System.Byte
    IL_0051:  starg.s    input
    IL_0053:  ldc.i4.0
    IL_0054:  starg.s    offset
    IL_0056:  ldarg.0
    IL_0057:  ldarg.1
    IL_0058:  ldelema    [mscorlib]System.Byte
    IL_005d:  stloc.1
    IL_005e:  ldarg.3
    IL_005f:  ldarg.s    outOffset
    IL_0061:  ldelema    [mscorlib]System.Byte
    IL_0066:  stloc.2
    IL_0067:  ldsfld     class Snappy.NativeProxy Snappy.NativeProxy::Instance
    IL_006c:  ldloc.1
    IL_006d:  conv.i
    IL_006e:  ldarg.2
    IL_006f:  ldloc.2
    IL_0070:  conv.i
    IL_0071:  ldloca.s   V_0
    IL_0073:  callvirt   instance valuetype Snappy.SnappyStatus Snappy.NativeProxy::Compress(uint8*,
                                                                                             int32,
                                                                                             uint8*,
                                                                                             int32&)
    IL_0078:  stloc.3
    IL_0079:  ldloc.3
    IL_007a:  brtrue.s   IL_0081

    IL_007c:  ldloc.0
    IL_007d:  stloc.s    V_4
    IL_007f:  leave.s    IL_009b

    IL_0081:  ldloc.3
    IL_0082:  ldc.i4.2
    IL_0083:  bne.un.s   IL_0090

    IL_0085:  ldstr      "Output array is too small"
    IL_008a:  newobj     instance void [mscorlib]System.ArgumentOutOfRangeException::.ctor(string)
    IL_008f:  throw

    IL_0090:  ldstr      "Invalid input"
    IL_0095:  newobj     instance void [System]System.IO.InvalidDataException::.ctor(string)
    IL_009a:  throw

    IL_009b:  ldloc.s    V_4
    IL_009d:  ret
  } // end of method SnappyCodec::Compress

  .method public hidebysig static uint8[] 
          Compress(uint8[] input) cil managed
  {
    // Code size       61 (0x3d)
    .maxstack  5
    .locals init (int32 V_0,
             uint8[] V_1,
             int32 V_2,
             uint8[] V_3)
    IL_0000:  ldarg.0
    IL_0001:  brtrue.s   IL_0009

    IL_0003:  newobj     instance void [mscorlib]System.ArgumentNullException::.ctor()
    IL_0008:  throw

    IL_0009:  ldarg.0
    IL_000a:  ldlen
    IL_000b:  conv.i4
    IL_000c:  call       int32 Snappy.SnappyCodec::GetMaxCompressedLength(int32)
    IL_0011:  stloc.0
    IL_0012:  ldloc.0
    IL_0013:  newarr     [mscorlib]System.Byte
    IL_0018:  stloc.1
    IL_0019:  ldarg.0
    IL_001a:  ldc.i4.0
    IL_001b:  ldarg.0
    IL_001c:  ldlen
    IL_001d:  conv.i4
    IL_001e:  ldloc.1
    IL_001f:  ldc.i4.0
    IL_0020:  call       int32 Snappy.SnappyCodec::Compress(uint8[],
                                                            int32,
                                                            int32,
                                                            uint8[],
                                                            int32)
    IL_0025:  stloc.2
    IL_0026:  ldloc.2
    IL_0027:  ldloc.0
    IL_0028:  bne.un.s   IL_002c

    IL_002a:  ldloc.1
    IL_002b:  ret

    IL_002c:  ldloc.2
    IL_002d:  newarr     [mscorlib]System.Byte
    IL_0032:  stloc.3
    IL_0033:  ldloc.1
    IL_0034:  ldloc.3
    IL_0035:  ldloc.2
    IL_0036:  call       void [mscorlib]System.Array::Copy(class [mscorlib]System.Array,
                                                           class [mscorlib]System.Array,
                                                           int32)
    IL_003b:  ldloc.3
    IL_003c:  ret
  } // end of method SnappyCodec::Compress

  .method public hidebysig static int32  Uncompress(uint8[] input,
                                                    int32 offset,
                                                    int32 length,
                                                    uint8[] output,
                                                    int32 outOffset) cil managed
  {
    // Code size       173 (0xad)
    .maxstack  5
    .locals init (int32 V_0,
             uint8& pinned V_1,
             uint8& pinned V_2,
             valuetype Snappy.SnappyStatus V_3,
             int32 V_4)
    IL_0000:  ldarg.0
    IL_0001:  brfalse.s  IL_0006

    IL_0003:  ldarg.3
    IL_0004:  brtrue.s   IL_000c

    IL_0006:  newobj     instance void [mscorlib]System.ArgumentNullException::.ctor()
    IL_000b:  throw

    IL_000c:  ldarg.1
    IL_000d:  ldc.i4.0
    IL_000e:  blt.s      IL_001c

    IL_0010:  ldarg.2
    IL_0011:  ldc.i4.0
    IL_0012:  blt.s      IL_001c

    IL_0014:  ldarg.1
    IL_0015:  ldarg.2
    IL_0016:  add
    IL_0017:  ldarg.0
    IL_0018:  ldlen
    IL_0019:  conv.i4
    IL_001a:  ble.s      IL_0027

    IL_001c:  ldstr      "Selected range is outside the bounds of the input "
    + "array"
    IL_0021:  newobj     instance void [mscorlib]System.ArgumentOutOfRangeException::.ctor(string)
    IL_0026:  throw

    IL_0027:  ldarg.2
    IL_0028:  brtrue.s   IL_0035

    IL_002a:  ldstr      "Compressed block cannot be empty"
    IL_002f:  newobj     instance void [System]System.IO.InvalidDataException::.ctor(string)
    IL_0034:  throw

    IL_0035:  ldarg.s    outOffset
    IL_0037:  ldc.i4.0
    IL_0038:  blt.s      IL_0041

    IL_003a:  ldarg.s    outOffset
    IL_003c:  ldarg.3
    IL_003d:  ldlen
    IL_003e:  conv.i4
    IL_003f:  ble.s      IL_004c

    IL_0041:  ldstr      "Output offset is outside the bounds of the output "
    + "array"
    IL_0046:  newobj     instance void [mscorlib]System.ArgumentOutOfRangeException::.ctor(string)
    IL_004b:  throw

    IL_004c:  ldarg.3
    IL_004d:  ldlen
    IL_004e:  conv.i4
    IL_004f:  ldarg.s    outOffset
    IL_0051:  sub
    IL_0052:  stloc.0
    IL_0053:  ldarg.s    outOffset
    IL_0055:  ldarg.3
    IL_0056:  ldlen
    IL_0057:  conv.i4
    IL_0058:  bne.un.s   IL_0065

    IL_005a:  ldc.i4.1
    IL_005b:  newarr     [mscorlib]System.Byte
    IL_0060:  starg.s    output
    IL_0062:  ldc.i4.0
    IL_0063:  starg.s    outOffset
    IL_0065:  ldarg.0
    IL_0066:  ldarg.1
    IL_0067:  ldelema    [mscorlib]System.Byte
    IL_006c:  stloc.1
    IL_006d:  ldarg.3
    IL_006e:  ldarg.s    outOffset
    IL_0070:  ldelema    [mscorlib]System.Byte
    IL_0075:  stloc.2
    IL_0076:  ldsfld     class Snappy.NativeProxy Snappy.NativeProxy::Instance
    IL_007b:  ldloc.1
    IL_007c:  conv.i
    IL_007d:  ldarg.2
    IL_007e:  ldloc.2
    IL_007f:  conv.i
    IL_0080:  ldloca.s   V_0
    IL_0082:  callvirt   instance valuetype Snappy.SnappyStatus Snappy.NativeProxy::Uncompress(uint8*,
                                                                                               int32,
                                                                                               uint8*,
                                                                                               int32&)
    IL_0087:  stloc.3
    IL_0088:  ldloc.3
    IL_0089:  brtrue.s   IL_0090

    IL_008b:  ldloc.0
    IL_008c:  stloc.s    V_4
    IL_008e:  leave.s    IL_00aa

    IL_0090:  ldloc.3
    IL_0091:  ldc.i4.2
    IL_0092:  bne.un.s   IL_009f

    IL_0094:  ldstr      "Output array is too small"
    IL_0099:  newobj     instance void [mscorlib]System.ArgumentOutOfRangeException::.ctor(string)
    IL_009e:  throw

    IL_009f:  ldstr      "Input is not a valid snappy-compressed block"
    IL_00a4:  newobj     instance void [System]System.IO.InvalidDataException::.ctor(string)
    IL_00a9:  throw

    IL_00aa:  ldloc.s    V_4
    IL_00ac:  ret
  } // end of method SnappyCodec::Uncompress

  .method public hidebysig static uint8[] 
          Uncompress(uint8[] input) cil managed
  {
    // Code size       50 (0x32)
    .maxstack  5
    .locals init (int32 V_0,
             uint8[] V_1,
             int32 V_2,
             uint8[] V_3)
    IL_0000:  ldarg.0
    IL_0001:  call       int32 Snappy.SnappyCodec::GetUncompressedLength(uint8[])
    IL_0006:  stloc.0
    IL_0007:  ldloc.0
    IL_0008:  newarr     [mscorlib]System.Byte
    IL_000d:  stloc.1
    IL_000e:  ldarg.0
    IL_000f:  ldc.i4.0
    IL_0010:  ldarg.0
    IL_0011:  ldlen
    IL_0012:  conv.i4
    IL_0013:  ldloc.1
    IL_0014:  ldc.i4.0
    IL_0015:  call       int32 Snappy.SnappyCodec::Uncompress(uint8[],
                                                              int32,
                                                              int32,
                                                              uint8[],
                                                              int32)
    IL_001a:  stloc.2
    IL_001b:  ldloc.2
    IL_001c:  ldloc.0
    IL_001d:  bne.un.s   IL_0021

    IL_001f:  ldloc.1
    IL_0020:  ret

    IL_0021:  ldloc.2
    IL_0022:  newarr     [mscorlib]System.Byte
    IL_0027:  stloc.3
    IL_0028:  ldloc.1
    IL_0029:  ldloc.3
    IL_002a:  ldloc.2
    IL_002b:  call       void [mscorlib]System.Array::Copy(class [mscorlib]System.Array,
                                                           class [mscorlib]System.Array,
                                                           int32)
    IL_0030:  ldloc.3
    IL_0031:  ret
  } // end of method SnappyCodec::Uncompress

  .method public hidebysig static int32  GetMaxCompressedLength(int32 inLength) cil managed
  {
    // Code size       12 (0xc)
    .maxstack  8
    IL_0000:  ldsfld     class Snappy.NativeProxy Snappy.NativeProxy::Instance
    IL_0005:  ldarg.0
    IL_0006:  callvirt   instance int32 Snappy.NativeProxy::GetMaxCompressedLength(int32)
    IL_000b:  ret
  } // end of method SnappyCodec::GetMaxCompressedLength

  .method public hidebysig static int32  GetUncompressedLength(uint8[] input,
                                                               int32 offset,
                                                               int32 length) cil managed
  {
    // Code size       94 (0x5e)
    .maxstack  4
    .locals init (uint8& pinned V_0,
             int32 V_1,
             valuetype Snappy.SnappyStatus V_2,
             int32 V_3)
    IL_0000:  ldarg.0
    IL_0001:  brtrue.s   IL_0009

    IL_0003:  newobj     instance void [mscorlib]System.ArgumentNullException::.ctor()
    IL_0008:  throw

    IL_0009:  ldarg.1
    IL_000a:  ldc.i4.0
    IL_000b:  blt.s      IL_0019

    IL_000d:  ldarg.2
    IL_000e:  ldc.i4.0
    IL_000f:  blt.s      IL_0019

    IL_0011:  ldarg.1
    IL_0012:  ldarg.2
    IL_0013:  add
    IL_0014:  ldarg.0
    IL_0015:  ldlen
    IL_0016:  conv.i4
    IL_0017:  ble.s      IL_0024

    IL_0019:  ldstr      "Selected range is outside the bounds of the input "
    + "array"
    IL_001e:  newobj     instance void [mscorlib]System.ArgumentOutOfRangeException::.ctor(string)
    IL_0023:  throw

    IL_0024:  ldarg.2
    IL_0025:  brtrue.s   IL_0032

    IL_0027:  ldstr      "Compressed block cannot be empty"
    IL_002c:  newobj     instance void [System]System.IO.InvalidDataException::.ctor(string)
    IL_0031:  throw

    IL_0032:  ldarg.0
    IL_0033:  ldarg.1
    IL_0034:  ldelema    [mscorlib]System.Byte
    IL_0039:  stloc.0
    IL_003a:  ldsfld     class Snappy.NativeProxy Snappy.NativeProxy::Instance
    IL_003f:  ldloc.0
    IL_0040:  conv.i
    IL_0041:  ldarg.2
    IL_0042:  ldloca.s   V_1
    IL_0044:  callvirt   instance valuetype Snappy.SnappyStatus Snappy.NativeProxy::GetUncompressedLength(uint8*,
                                                                                                          int32,
                                                                                                          int32&)
    IL_0049:  stloc.2
    IL_004a:  ldloc.2
    IL_004b:  brtrue.s   IL_0051

    IL_004d:  ldloc.1
    IL_004e:  stloc.3
    IL_004f:  leave.s    IL_005c

    IL_0051:  ldstr      "Input is not a valid snappy-compressed block"
    IL_0056:  newobj     instance void [System]System.IO.InvalidDataException::.ctor(string)
    IL_005b:  throw

    IL_005c:  ldloc.3
    IL_005d:  ret
  } // end of method SnappyCodec::GetUncompressedLength

  .method public hidebysig static int32  GetUncompressedLength(uint8[] input) cil managed
  {
    // Code size       20 (0x14)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  brtrue.s   IL_0009

    IL_0003:  newobj     instance void [mscorlib]System.ArgumentNullException::.ctor()
    IL_0008:  throw

    IL_0009:  ldarg.0
    IL_000a:  ldc.i4.0
    IL_000b:  ldarg.0
    IL_000c:  ldlen
    IL_000d:  conv.i4
    IL_000e:  call       int32 Snappy.SnappyCodec::GetUncompressedLength(uint8[],
                                                                         int32,
                                                                         int32)
    IL_0013:  ret
  } // end of method SnappyCodec::GetUncompressedLength

  .method public hidebysig static bool  Validate(uint8[] input,
                                                 int32 offset,
                                                 int32 length) cil managed
  {
    // Code size       70 (0x46)
    .maxstack  3
    .locals init (uint8& pinned V_0,
             bool V_1)
    IL_0000:  ldarg.0
    IL_0001:  brtrue.s   IL_0009

    IL_0003:  newobj     instance void [mscorlib]System.ArgumentNullException::.ctor()
    IL_0008:  throw

    IL_0009:  ldarg.1
    IL_000a:  ldc.i4.0
    IL_000b:  blt.s      IL_0019

    IL_000d:  ldarg.2
    IL_000e:  ldc.i4.0
    IL_000f:  blt.s      IL_0019

    IL_0011:  ldarg.1
    IL_0012:  ldarg.2
    IL_0013:  add
    IL_0014:  ldarg.0
    IL_0015:  ldlen
    IL_0016:  conv.i4
    IL_0017:  ble.s      IL_0024

    IL_0019:  ldstr      "Selected range is outside the bounds of the input "
    + "array"
    IL_001e:  newobj     instance void [mscorlib]System.ArgumentOutOfRangeException::.ctor(string)
    IL_0023:  throw

    IL_0024:  ldarg.2
    IL_0025:  brtrue.s   IL_0029

    IL_0027:  ldc.i4.0
    IL_0028:  ret

    IL_0029:  ldarg.0
    IL_002a:  ldarg.1
    IL_002b:  ldelema    [mscorlib]System.Byte
    IL_0030:  stloc.0
    IL_0031:  ldsfld     class Snappy.NativeProxy Snappy.NativeProxy::Instance
    IL_0036:  ldloc.0
    IL_0037:  conv.i
    IL_0038:  ldarg.2
    IL_0039:  callvirt   instance valuetype Snappy.SnappyStatus Snappy.NativeProxy::ValidateCompressedBuffer(uint8*,
                                                                                                             int32)
    IL_003e:  ldc.i4.0
    IL_003f:  ceq
    IL_0041:  stloc.1
    IL_0042:  leave.s    IL_0044

    IL_0044:  ldloc.1
    IL_0045:  ret
  } // end of method SnappyCodec::Validate

  .method public hidebysig static bool  Validate(uint8[] input) cil managed
  {
    // Code size       20 (0x14)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  brtrue.s   IL_0009

    IL_0003:  newobj     instance void [mscorlib]System.ArgumentNullException::.ctor()
    IL_0008:  throw

    IL_0009:  ldarg.0
    IL_000a:  ldc.i4.0
    IL_000b:  ldarg.0
    IL_000c:  ldlen
    IL_000d:  conv.i4
    IL_000e:  call       bool Snappy.SnappyCodec::Validate(uint8[],
                                                           int32,
                                                           int32)
    IL_0013:  ret
  } // end of method SnappyCodec::Validate

} // end of class Snappy.SnappyCodec

.class public auto ansi beforefieldinit Snappy.SnappyFrame
       extends [mscorlib]System.Object
{
  .field public static literal int32 MaxFrameSize = int32(0x00010000)
  .field public initonly uint8[] StreamIdentifier
  .field private static initonly int32 MaxBufferUsage
  .field private uint8[] WordBuffer
  .field private uint8[] Buffer
  .field private int32 BufferUsage
  .field private valuetype Snappy.SnappyFrameType '<Type>k__BackingField'
  .custom instance void [mscorlib]System.Runtime.CompilerServices.CompilerGeneratedAttribute::.ctor() = ( 01 00 00 00 ) 
  .field private uint32 '<Checksum>k__BackingField'
  .custom instance void [mscorlib]System.Runtime.CompilerServices.CompilerGeneratedAttribute::.ctor() = ( 01 00 00 00 ) 
  .field private int32 '<DataLength>k__BackingField'
  .custom instance void [mscorlib]System.Runtime.CompilerServices.CompilerGeneratedAttribute::.ctor() = ( 01 00 00 00 ) 
  .method public hidebysig specialname instance valuetype Snappy.SnappyFrameType 
          get_Type() cil managed
  {
    .custom instance void [mscorlib]System.Runtime.CompilerServices.CompilerGeneratedAttribute::.ctor() = ( 01 00 00 00 ) 
    // Code size       7 (0x7)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldfld      valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::'<Type>k__BackingField'
    IL_0006:  ret
  } // end of method SnappyFrame::get_Type

  .method private hidebysig specialname instance void 
          set_Type(valuetype Snappy.SnappyFrameType 'value') cil managed
  {
    .custom instance void [mscorlib]System.Runtime.CompilerServices.CompilerGeneratedAttribute::.ctor() = ( 01 00 00 00 ) 
    // Code size       8 (0x8)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldarg.1
    IL_0002:  stfld      valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::'<Type>k__BackingField'
    IL_0007:  ret
  } // end of method SnappyFrame::set_Type

  .method public hidebysig specialname instance uint32 
          get_Checksum() cil managed
  {
    .custom instance void [mscorlib]System.Runtime.CompilerServices.CompilerGeneratedAttribute::.ctor() = ( 01 00 00 00 ) 
    // Code size       7 (0x7)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldfld      uint32 Snappy.SnappyFrame::'<Checksum>k__BackingField'
    IL_0006:  ret
  } // end of method SnappyFrame::get_Checksum

  .method private hidebysig specialname instance void 
          set_Checksum(uint32 'value') cil managed
  {
    .custom instance void [mscorlib]System.Runtime.CompilerServices.CompilerGeneratedAttribute::.ctor() = ( 01 00 00 00 ) 
    // Code size       8 (0x8)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldarg.1
    IL_0002:  stfld      uint32 Snappy.SnappyFrame::'<Checksum>k__BackingField'
    IL_0007:  ret
  } // end of method SnappyFrame::set_Checksum

  .method public hidebysig specialname instance int32 
          get_DataLength() cil managed
  {
    .custom instance void [mscorlib]System.Runtime.CompilerServices.CompilerGeneratedAttribute::.ctor() = ( 01 00 00 00 ) 
    // Code size       7 (0x7)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldfld      int32 Snappy.SnappyFrame::'<DataLength>k__BackingField'
    IL_0006:  ret
  } // end of method SnappyFrame::get_DataLength

  .method private hidebysig specialname instance void 
          set_DataLength(int32 'value') cil managed
  {
    .custom instance void [mscorlib]System.Runtime.CompilerServices.CompilerGeneratedAttribute::.ctor() = ( 01 00 00 00 ) 
    // Code size       8 (0x8)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldarg.1
    IL_0002:  stfld      int32 Snappy.SnappyFrame::'<DataLength>k__BackingField'
    IL_0007:  ret
  } // end of method SnappyFrame::set_DataLength

  .method public hidebysig specialname instance uint8[] 
          get_Data() cil managed
  {
    // Code size       44 (0x2c)
    .maxstack  2
    .locals init (uint8[] V_0)
    IL_0000:  ldarg.0
    IL_0001:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_0006:  brfalse.s  IL_0017

    IL_0008:  ldarg.0
    IL_0009:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_000e:  ldc.i4.1
    IL_000f:  beq.s      IL_0017

    IL_0011:  newobj     instance void [mscorlib]System.InvalidOperationException::.ctor()
    IL_0016:  throw

    IL_0017:  ldarg.0
    IL_0018:  call       instance int32 Snappy.SnappyFrame::get_DataLength()
    IL_001d:  newarr     [mscorlib]System.Byte
    IL_0022:  stloc.0
    IL_0023:  ldarg.0
    IL_0024:  ldloc.0
    IL_0025:  call       instance void Snappy.SnappyFrame::GetData(uint8[])
    IL_002a:  ldloc.0
    IL_002b:  ret
  } // end of method SnappyFrame::get_Data

  .method public hidebysig specialname rtspecialname 
          instance void  .ctor() cil managed
  {
    // Code size       47 (0x2f)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  call       class [mscorlib]System.Text.Encoding [mscorlib]System.Text.Encoding::get_ASCII()
    IL_0006:  ldstr      "sNaPpY"
    IL_000b:  callvirt   instance uint8[] [mscorlib]System.Text.Encoding::GetBytes(string)
    IL_0010:  stfld      uint8[] Snappy.SnappyFrame::StreamIdentifier
    IL_0015:  ldarg.0
    IL_0016:  ldc.i4.4
    IL_0017:  newarr     [mscorlib]System.Byte
    IL_001c:  stfld      uint8[] Snappy.SnappyFrame::WordBuffer
    IL_0021:  ldarg.0
    IL_0022:  call       instance void [mscorlib]System.Object::.ctor()
    IL_0027:  ldarg.0
    IL_0028:  ldc.i4.0
    IL_0029:  call       instance void Snappy.SnappyFrame::SetPadding(int32)
    IL_002e:  ret
  } // end of method SnappyFrame::.ctor

  .method public hidebysig instance void 
          SetStreamIdentifier() cil managed
  {
    // Code size       33 (0x21)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldc.i4.0
    IL_0002:  stfld      int32 Snappy.SnappyFrame::BufferUsage
    IL_0007:  ldarg.0
    IL_0008:  ldc.i4.0
    IL_0009:  call       instance void Snappy.SnappyFrame::set_Checksum(uint32)
    IL_000e:  ldarg.0
    IL_000f:  ldc.i4.6
    IL_0010:  call       instance void Snappy.SnappyFrame::set_DataLength(int32)
    IL_0015:  ldarg.0
    IL_0016:  ldc.i4     0xff
    IL_001b:  call       instance void Snappy.SnappyFrame::set_Type(valuetype Snappy.SnappyFrameType)
    IL_0020:  ret
  } // end of method SnappyFrame::SetStreamIdentifier

  .method public hidebysig instance void 
          SetPadding(int32 size) cil managed
  {
    // Code size       51 (0x33)
    .maxstack  8
    IL_0000:  ldarg.1
    IL_0001:  ldc.i4.0
    IL_0002:  blt.s      IL_000c

    IL_0004:  ldarg.1
    IL_0005:  ldc.i4     0x1000000
    IL_000a:  blt.s      IL_0012

    IL_000c:  newobj     instance void [mscorlib]System.ArgumentOutOfRangeException::.ctor()
    IL_0011:  throw

    IL_0012:  ldarg.0
    IL_0013:  ldc.i4.0
    IL_0014:  stfld      int32 Snappy.SnappyFrame::BufferUsage
    IL_0019:  ldarg.0
    IL_001a:  ldc.i4.0
    IL_001b:  call       instance void Snappy.SnappyFrame::set_Checksum(uint32)
    IL_0020:  ldarg.0
    IL_0021:  ldarg.1
    IL_0022:  call       instance void Snappy.SnappyFrame::set_DataLength(int32)
    IL_0027:  ldarg.0
    IL_0028:  ldc.i4     0xfe
    IL_002d:  call       instance void Snappy.SnappyFrame::set_Type(valuetype Snappy.SnappyFrameType)
    IL_0032:  ret
  } // end of method SnappyFrame::SetPadding

  .method public hidebysig instance void 
          SetCompressed(uint8[] data) cil managed
  {
    // Code size       12 (0xc)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldarg.1
    IL_0002:  ldc.i4.0
    IL_0003:  ldarg.1
    IL_0004:  ldlen
    IL_0005:  conv.i4
    IL_0006:  call       instance void Snappy.SnappyFrame::SetCompressed(uint8[],
                                                                         int32,
                                                                         int32)
    IL_000b:  ret
  } // end of method SnappyFrame::SetCompressed

  .method public hidebysig instance void 
          SetCompressed(uint8[] data,
                        int32 offset,
                        int32 count) cil managed
  {
    // Code size       78 (0x4e)
    .maxstack  6
    IL_0000:  ldarg.0
    IL_0001:  ldarg.1
    IL_0002:  ldarg.2
    IL_0003:  ldarg.3
    IL_0004:  call       instance void Snappy.SnappyFrame::CheckRange(uint8[],
                                                                      int32,
                                                                      int32)
    IL_0009:  ldarg.0
    IL_000a:  ldarg.3
    IL_000b:  call       instance void Snappy.SnappyFrame::CheckMaxFrameSize(int32)
    IL_0010:  ldarg.0
    IL_0011:  ldarg.3
    IL_0012:  call       int32 Snappy.SnappyCodec::GetMaxCompressedLength(int32)
    IL_0017:  call       instance void Snappy.SnappyFrame::EnsureBuffer(int32)
    IL_001c:  ldarg.0
    IL_001d:  ldarg.1
    IL_001e:  ldarg.2
    IL_001f:  ldarg.3
    IL_0020:  ldarg.0
    IL_0021:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_0026:  ldc.i4.0
    IL_0027:  call       int32 Snappy.SnappyCodec::Compress(uint8[],
                                                            int32,
                                                            int32,
                                                            uint8[],
                                                            int32)
    IL_002c:  stfld      int32 Snappy.SnappyFrame::BufferUsage
    IL_0031:  ldarg.0
    IL_0032:  ldarg.3
    IL_0033:  call       instance void Snappy.SnappyFrame::set_DataLength(int32)
    IL_0038:  ldarg.0
    IL_0039:  ldarg.1
    IL_003a:  ldarg.2
    IL_003b:  ldarg.3
    IL_003c:  call       uint32 Snappy.SnappyFrame::ComputeMasked(uint8[],
                                                                  int32,
                                                                  int32)
    IL_0041:  call       instance void Snappy.SnappyFrame::set_Checksum(uint32)
    IL_0046:  ldarg.0
    IL_0047:  ldc.i4.0
    IL_0048:  call       instance void Snappy.SnappyFrame::set_Type(valuetype Snappy.SnappyFrameType)
    IL_004d:  ret
  } // end of method SnappyFrame::SetCompressed

  .method public hidebysig instance void 
          SetUncompressed(uint8[] data) cil managed
  {
    // Code size       12 (0xc)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldarg.1
    IL_0002:  ldc.i4.0
    IL_0003:  ldarg.1
    IL_0004:  ldlen
    IL_0005:  conv.i4
    IL_0006:  call       instance void Snappy.SnappyFrame::SetUncompressed(uint8[],
                                                                           int32,
                                                                           int32)
    IL_000b:  ret
  } // end of method SnappyFrame::SetUncompressed

  .method public hidebysig instance void 
          SetUncompressed(uint8[] data,
                          int32 offset,
                          int32 count) cil managed
  {
    // Code size       74 (0x4a)
    .maxstack  5
    IL_0000:  ldarg.0
    IL_0001:  ldarg.1
    IL_0002:  ldarg.2
    IL_0003:  ldarg.3
    IL_0004:  call       instance void Snappy.SnappyFrame::CheckRange(uint8[],
                                                                      int32,
                                                                      int32)
    IL_0009:  ldarg.0
    IL_000a:  ldarg.3
    IL_000b:  call       instance void Snappy.SnappyFrame::CheckMaxFrameSize(int32)
    IL_0010:  ldarg.0
    IL_0011:  ldarg.3
    IL_0012:  call       instance void Snappy.SnappyFrame::EnsureBuffer(int32)
    IL_0017:  ldarg.1
    IL_0018:  ldarg.2
    IL_0019:  ldarg.0
    IL_001a:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_001f:  ldc.i4.0
    IL_0020:  ldarg.3
    IL_0021:  call       void [mscorlib]System.Array::Copy(class [mscorlib]System.Array,
                                                           int32,
                                                           class [mscorlib]System.Array,
                                                           int32,
                                                           int32)
    IL_0026:  ldarg.0
    IL_0027:  ldarg.3
    IL_0028:  stfld      int32 Snappy.SnappyFrame::BufferUsage
    IL_002d:  ldarg.0
    IL_002e:  ldarg.3
    IL_002f:  call       instance void Snappy.SnappyFrame::set_DataLength(int32)
    IL_0034:  ldarg.0
    IL_0035:  ldarg.1
    IL_0036:  ldarg.2
    IL_0037:  ldarg.3
    IL_0038:  call       uint32 Snappy.SnappyFrame::ComputeMasked(uint8[],
                                                                  int32,
                                                                  int32)
    IL_003d:  call       instance void Snappy.SnappyFrame::set_Checksum(uint32)
    IL_0042:  ldarg.0
    IL_0043:  ldc.i4.1
    IL_0044:  call       instance void Snappy.SnappyFrame::set_Type(valuetype Snappy.SnappyFrameType)
    IL_0049:  ret
  } // end of method SnappyFrame::SetUncompressed

  .method public hidebysig instance void 
          GetData(uint8[] buffer) cil managed
  {
    // Code size       9 (0x9)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldarg.1
    IL_0002:  ldc.i4.0
    IL_0003:  call       instance void Snappy.SnappyFrame::GetData(uint8[],
                                                                   int32)
    IL_0008:  ret
  } // end of method SnappyFrame::GetData

  .method public hidebysig instance void 
          GetData(uint8[] buffer,
                  int32 offset) cil managed
  {
    // Code size       120 (0x78)
    .maxstack  5
    .locals init (int32 V_0)
    IL_0000:  ldarg.0
    IL_0001:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_0006:  brtrue.s   IL_0033

    IL_0008:  ldarg.0
    IL_0009:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_000e:  ldc.i4.0
    IL_000f:  ldarg.0
    IL_0010:  ldfld      int32 Snappy.SnappyFrame::BufferUsage
    IL_0015:  ldarg.1
    IL_0016:  ldarg.2
    IL_0017:  call       int32 Snappy.SnappyCodec::Uncompress(uint8[],
                                                              int32,
                                                              int32,
                                                              uint8[],
                                                              int32)
    IL_001c:  stloc.0
    IL_001d:  ldarg.1
    IL_001e:  ldarg.2
    IL_001f:  ldloc.0
    IL_0020:  call       uint32 Snappy.SnappyFrame::ComputeMasked(uint8[],
                                                                  int32,
                                                                  int32)
    IL_0025:  ldarg.0
    IL_0026:  call       instance uint32 Snappy.SnappyFrame::get_Checksum()
    IL_002b:  beq.s      IL_0077

    IL_002d:  newobj     instance void [System]System.IO.InvalidDataException::.ctor()
    IL_0032:  throw

    IL_0033:  ldarg.0
    IL_0034:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_0039:  ldc.i4.1
    IL_003a:  bne.un.s   IL_0071

    IL_003c:  ldarg.0
    IL_003d:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_0042:  ldarg.2
    IL_0043:  ldarg.0
    IL_0044:  ldfld      int32 Snappy.SnappyFrame::BufferUsage
    IL_0049:  call       uint32 Snappy.SnappyFrame::ComputeMasked(uint8[],
                                                                  int32,
                                                                  int32)
    IL_004e:  ldarg.0
    IL_004f:  call       instance uint32 Snappy.SnappyFrame::get_Checksum()
    IL_0054:  beq.s      IL_005c

    IL_0056:  newobj     instance void [System]System.IO.InvalidDataException::.ctor()
    IL_005b:  throw

    IL_005c:  ldarg.0
    IL_005d:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_0062:  ldc.i4.0
    IL_0063:  ldarg.1
    IL_0064:  ldarg.2
    IL_0065:  ldarg.0
    IL_0066:  ldfld      int32 Snappy.SnappyFrame::BufferUsage
    IL_006b:  call       void [mscorlib]System.Array::Copy(class [mscorlib]System.Array,
                                                           int32,
                                                           class [mscorlib]System.Array,
                                                           int32,
                                                           int32)
    IL_0070:  ret

    IL_0071:  newobj     instance void [mscorlib]System.InvalidOperationException::.ctor()
    IL_0076:  throw

    IL_0077:  ret
  } // end of method SnappyFrame::GetData

  .method public hidebysig instance bool 
          Read(class [mscorlib]System.IO.Stream 'stream') cil managed
  {
    // Code size       489 (0x1e9)
    .maxstack  6
    .locals init (int32 V_0,
             int32 V_1,
             bool V_2)
    .try
    {
      IL_0000:  ldarg.1
      IL_0001:  ldarg.0
      IL_0002:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
      IL_0007:  ldc.i4.0
      IL_0008:  ldc.i4.4
      IL_0009:  callvirt   instance int32 [mscorlib]System.IO.Stream::Read(uint8[],
                                                                           int32,
                                                                           int32)
      IL_000e:  stloc.0
      IL_000f:  ldloc.0
      IL_0010:  brtrue.s   IL_0019

      IL_0012:  ldc.i4.0
      IL_0013:  stloc.2
      IL_0014:  leave      IL_01e7

      IL_0019:  ldarg.0
      IL_001a:  ldarg.1
      IL_001b:  ldarg.0
      IL_001c:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
      IL_0021:  ldloc.0
      IL_0022:  ldc.i4.4
      IL_0023:  ldloc.0
      IL_0024:  sub
      IL_0025:  call       instance void Snappy.SnappyFrame::EnsureRead(class [mscorlib]System.IO.Stream,
                                                                        uint8[],
                                                                        int32,
                                                                        int32)
      IL_002a:  ldarg.0
      IL_002b:  ldarg.0
      IL_002c:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
      IL_0031:  ldc.i4.0
      IL_0032:  ldelem.u1
      IL_0033:  call       instance void Snappy.SnappyFrame::set_Type(valuetype Snappy.SnappyFrameType)
      IL_0038:  ldarg.0
      IL_0039:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
      IL_003e:  ldc.i4.1
      IL_003f:  ldelem.u1
      IL_0040:  ldarg.0
      IL_0041:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
      IL_0046:  ldc.i4.2
      IL_0047:  ldelem.u1
      IL_0048:  ldc.i4.8
      IL_0049:  shl
      IL_004a:  add
      IL_004b:  ldarg.0
      IL_004c:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
      IL_0051:  ldc.i4.3
      IL_0052:  ldelem.u1
      IL_0053:  ldc.i4.s   16
      IL_0055:  shl
      IL_0056:  add
      IL_0057:  stloc.1
      IL_0058:  ldarg.0
      IL_0059:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
      IL_005e:  brfalse.s  IL_006c

      IL_0060:  ldarg.0
      IL_0061:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
      IL_0066:  ldc.i4.1
      IL_0067:  bne.un     IL_0131

      IL_006c:  ldloc.1
      IL_006d:  ldc.i4.4
      IL_006e:  bge.s      IL_0076

      IL_0070:  newobj     instance void [System]System.IO.InvalidDataException::.ctor()
      IL_0075:  throw

      IL_0076:  ldarg.0
      IL_0077:  ldarg.1
      IL_0078:  ldarg.0
      IL_0079:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
      IL_007e:  ldc.i4.0
      IL_007f:  ldc.i4.4
      IL_0080:  call       instance void Snappy.SnappyFrame::EnsureRead(class [mscorlib]System.IO.Stream,
                                                                        uint8[],
                                                                        int32,
                                                                        int32)
      IL_0085:  ldarg.0
      IL_0086:  ldarg.0
      IL_0087:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
      IL_008c:  ldc.i4.0
      IL_008d:  ldelem.u1
      IL_008e:  ldarg.0
      IL_008f:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
      IL_0094:  ldc.i4.1
      IL_0095:  ldelem.u1
      IL_0096:  ldc.i4.8
      IL_0097:  shl
      IL_0098:  add
      IL_0099:  ldarg.0
      IL_009a:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
      IL_009f:  ldc.i4.2
      IL_00a0:  ldelem.u1
      IL_00a1:  ldc.i4.s   16
      IL_00a3:  shl
      IL_00a4:  add
      IL_00a5:  ldarg.0
      IL_00a6:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
      IL_00ab:  ldc.i4.3
      IL_00ac:  ldelem.u1
      IL_00ad:  ldc.i4.s   24
      IL_00af:  shl
      IL_00b0:  add
      IL_00b1:  call       instance void Snappy.SnappyFrame::set_Checksum(uint32)
      IL_00b6:  ldarg.0
      IL_00b7:  ldloc.1
      IL_00b8:  ldc.i4.4
      IL_00b9:  sub
      IL_00ba:  stfld      int32 Snappy.SnappyFrame::BufferUsage
      IL_00bf:  ldarg.0
      IL_00c0:  ldfld      int32 Snappy.SnappyFrame::BufferUsage
      IL_00c5:  ldsfld     int32 Snappy.SnappyFrame::MaxBufferUsage
      IL_00ca:  ble.s      IL_00d2

      IL_00cc:  newobj     instance void [System]System.IO.InvalidDataException::.ctor()
      IL_00d1:  throw

      IL_00d2:  ldarg.0
      IL_00d3:  ldarg.0
      IL_00d4:  ldfld      int32 Snappy.SnappyFrame::BufferUsage
      IL_00d9:  call       instance void Snappy.SnappyFrame::EnsureBuffer(int32)
      IL_00de:  ldarg.0
      IL_00df:  ldarg.1
      IL_00e0:  ldarg.0
      IL_00e1:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
      IL_00e6:  ldc.i4.0
      IL_00e7:  ldarg.0
      IL_00e8:  ldfld      int32 Snappy.SnappyFrame::BufferUsage
      IL_00ed:  call       instance void Snappy.SnappyFrame::EnsureRead(class [mscorlib]System.IO.Stream,
                                                                        uint8[],
                                                                        int32,
                                                                        int32)
      IL_00f2:  ldarg.0
      IL_00f3:  ldarg.0
      IL_00f4:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
      IL_00f9:  ldc.i4.1
      IL_00fa:  beq.s      IL_0110

      IL_00fc:  ldarg.0
      IL_00fd:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
      IL_0102:  ldc.i4.0
      IL_0103:  ldarg.0
      IL_0104:  ldfld      int32 Snappy.SnappyFrame::BufferUsage
      IL_0109:  call       int32 Snappy.SnappyCodec::GetUncompressedLength(uint8[],
                                                                           int32,
                                                                           int32)
      IL_010e:  br.s       IL_0116

      IL_0110:  ldarg.0
      IL_0111:  ldfld      int32 Snappy.SnappyFrame::BufferUsage
      IL_0116:  call       instance void Snappy.SnappyFrame::set_DataLength(int32)
      IL_011b:  ldarg.0
      IL_011c:  call       instance int32 Snappy.SnappyFrame::get_DataLength()
      IL_0121:  ldc.i4     0x10000
      IL_0126:  ble        IL_01d9

      IL_012b:  newobj     instance void [System]System.IO.InvalidDataException::.ctor()
      IL_0130:  throw

      IL_0131:  ldarg.0
      IL_0132:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
      IL_0137:  ldc.i4     0xfe
      IL_013c:  beq.s      IL_0158

      IL_013e:  ldarg.0
      IL_013f:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
      IL_0144:  ldc.i4     0x80
      IL_0149:  blt.s      IL_0177

      IL_014b:  ldarg.0
      IL_014c:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
      IL_0151:  ldc.i4     0xfd
      IL_0156:  bgt.s      IL_0177

      IL_0158:  ldarg.0
      IL_0159:  ldloc.1
      IL_015a:  call       instance void Snappy.SnappyFrame::set_DataLength(int32)
      IL_015f:  ldarg.0
      IL_0160:  ldc.i4.0
      IL_0161:  stfld      int32 Snappy.SnappyFrame::BufferUsage
      IL_0166:  ldarg.0
      IL_0167:  ldc.i4.0
      IL_0168:  call       instance void Snappy.SnappyFrame::set_Checksum(uint32)
      IL_016d:  ldarg.0
      IL_016e:  ldarg.1
      IL_016f:  ldloc.1
      IL_0170:  call       instance void Snappy.SnappyFrame::SkipBytes(class [mscorlib]System.IO.Stream,
                                                                       int32)
      IL_0175:  br.s       IL_01d9

      IL_0177:  ldarg.0
      IL_0178:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
      IL_017d:  ldc.i4     0xff
      IL_0182:  bne.un.s   IL_01d3

      IL_0184:  ldloc.1
      IL_0185:  ldc.i4.6
      IL_0186:  beq.s      IL_018e

      IL_0188:  newobj     instance void [mscorlib]System.InvalidOperationException::.ctor()
      IL_018d:  throw

      IL_018e:  ldarg.0
      IL_018f:  ldc.i4.6
      IL_0190:  call       instance void Snappy.SnappyFrame::set_DataLength(int32)
      IL_0195:  ldarg.0
      IL_0196:  ldc.i4.0
      IL_0197:  stfld      int32 Snappy.SnappyFrame::BufferUsage
      IL_019c:  ldarg.0
      IL_019d:  ldc.i4.0
      IL_019e:  call       instance void Snappy.SnappyFrame::set_Checksum(uint32)
      IL_01a3:  ldarg.0
      IL_01a4:  ldc.i4.6
      IL_01a5:  call       instance void Snappy.SnappyFrame::EnsureBuffer(int32)
      IL_01aa:  ldarg.0
      IL_01ab:  ldarg.1
      IL_01ac:  ldarg.0
      IL_01ad:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
      IL_01b2:  ldc.i4.0
      IL_01b3:  ldc.i4.6
      IL_01b4:  call       instance void Snappy.SnappyFrame::EnsureRead(class [mscorlib]System.IO.Stream,
                                                                        uint8[],
                                                                        int32,
                                                                        int32)
      IL_01b9:  ldarg.0
      IL_01ba:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
      IL_01bf:  ldarg.0
      IL_01c0:  ldfld      uint8[] Snappy.SnappyFrame::StreamIdentifier
      IL_01c5:  ldc.i4.6
      IL_01c6:  call       bool Snappy.Utils::BuffersEqual(uint8[],
                                                           uint8[],
                                                           int32)
      IL_01cb:  brtrue.s   IL_01d9

      IL_01cd:  newobj     instance void [System]System.IO.InvalidDataException::.ctor()
      IL_01d2:  throw

      IL_01d3:  newobj     instance void [System]System.IO.InvalidDataException::.ctor()
      IL_01d8:  throw

      IL_01d9:  ldc.i4.1
      IL_01da:  stloc.2
      IL_01db:  leave.s    IL_01e7

    }  // end .try
    catch [mscorlib]System.Object 
    {
      IL_01dd:  pop
      IL_01de:  ldarg.0
      IL_01df:  ldc.i4.0
      IL_01e0:  call       instance void Snappy.SnappyFrame::SetPadding(int32)
      IL_01e5:  rethrow
    }  // end handler
    IL_01e7:  ldloc.2
    IL_01e8:  ret
  } // end of method SnappyFrame::Read

  .method public hidebysig instance void 
          Write(class [mscorlib]System.IO.Stream 'stream') cil managed
  {
    // Code size       320 (0x140)
    .maxstack  4
    .locals init (int32 V_0)
    IL_0000:  ldarg.0
    IL_0001:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_0006:  brfalse.s  IL_0031

    IL_0008:  ldarg.0
    IL_0009:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_000e:  ldc.i4.1
    IL_000f:  beq.s      IL_0031

    IL_0011:  ldarg.0
    IL_0012:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_0017:  ldc.i4     0xff
    IL_001c:  beq.s      IL_0031

    IL_001e:  ldarg.0
    IL_001f:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_0024:  ldc.i4     0xfe
    IL_0029:  beq.s      IL_0031

    IL_002b:  newobj     instance void [mscorlib]System.InvalidOperationException::.ctor()
    IL_0030:  throw

    IL_0031:  ldarg.0
    IL_0032:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_0037:  brfalse.s  IL_004a

    IL_0039:  ldarg.0
    IL_003a:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_003f:  ldc.i4.1
    IL_0040:  beq.s      IL_004a

    IL_0042:  ldarg.0
    IL_0043:  call       instance int32 Snappy.SnappyFrame::get_DataLength()
    IL_0048:  br.s       IL_0052

    IL_004a:  ldarg.0
    IL_004b:  ldfld      int32 Snappy.SnappyFrame::BufferUsage
    IL_0050:  ldc.i4.4
    IL_0051:  add
    IL_0052:  stloc.0
    IL_0053:  ldarg.0
    IL_0054:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
    IL_0059:  ldc.i4.0
    IL_005a:  ldarg.0
    IL_005b:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_0060:  stelem.i1
    IL_0061:  ldarg.0
    IL_0062:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
    IL_0067:  ldc.i4.1
    IL_0068:  ldloc.0
    IL_0069:  conv.u1
    IL_006a:  stelem.i1
    IL_006b:  ldarg.0
    IL_006c:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
    IL_0071:  ldc.i4.2
    IL_0072:  ldloc.0
    IL_0073:  ldc.i4.8
    IL_0074:  shr
    IL_0075:  conv.u1
    IL_0076:  stelem.i1
    IL_0077:  ldarg.0
    IL_0078:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
    IL_007d:  ldc.i4.3
    IL_007e:  ldloc.0
    IL_007f:  ldc.i4.s   16
    IL_0081:  shr
    IL_0082:  conv.u1
    IL_0083:  stelem.i1
    IL_0084:  ldarg.1
    IL_0085:  ldarg.0
    IL_0086:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
    IL_008b:  ldc.i4.0
    IL_008c:  ldc.i4.4
    IL_008d:  callvirt   instance void [mscorlib]System.IO.Stream::Write(uint8[],
                                                                         int32,
                                                                         int32)
    IL_0092:  ldarg.0
    IL_0093:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_0098:  brfalse.s  IL_00a3

    IL_009a:  ldarg.0
    IL_009b:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_00a0:  ldc.i4.1
    IL_00a1:  bne.un.s   IL_00f5

    IL_00a3:  ldarg.0
    IL_00a4:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
    IL_00a9:  ldc.i4.0
    IL_00aa:  ldarg.0
    IL_00ab:  call       instance uint32 Snappy.SnappyFrame::get_Checksum()
    IL_00b0:  conv.u1
    IL_00b1:  stelem.i1
    IL_00b2:  ldarg.0
    IL_00b3:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
    IL_00b8:  ldc.i4.1
    IL_00b9:  ldarg.0
    IL_00ba:  call       instance uint32 Snappy.SnappyFrame::get_Checksum()
    IL_00bf:  ldc.i4.8
    IL_00c0:  shr.un
    IL_00c1:  conv.u1
    IL_00c2:  stelem.i1
    IL_00c3:  ldarg.0
    IL_00c4:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
    IL_00c9:  ldc.i4.2
    IL_00ca:  ldarg.0
    IL_00cb:  call       instance uint32 Snappy.SnappyFrame::get_Checksum()
    IL_00d0:  ldc.i4.s   16
    IL_00d2:  shr.un
    IL_00d3:  conv.u1
    IL_00d4:  stelem.i1
    IL_00d5:  ldarg.0
    IL_00d6:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
    IL_00db:  ldc.i4.3
    IL_00dc:  ldarg.0
    IL_00dd:  call       instance uint32 Snappy.SnappyFrame::get_Checksum()
    IL_00e2:  ldc.i4.s   24
    IL_00e4:  shr.un
    IL_00e5:  conv.u1
    IL_00e6:  stelem.i1
    IL_00e7:  ldarg.1
    IL_00e8:  ldarg.0
    IL_00e9:  ldfld      uint8[] Snappy.SnappyFrame::WordBuffer
    IL_00ee:  ldc.i4.0
    IL_00ef:  ldc.i4.4
    IL_00f0:  callvirt   instance void [mscorlib]System.IO.Stream::Write(uint8[],
                                                                         int32,
                                                                         int32)
    IL_00f5:  ldarg.0
    IL_00f6:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_00fb:  ldc.i4     0xff
    IL_0100:  bne.un.s   IL_0111

    IL_0102:  ldarg.1
    IL_0103:  ldarg.0
    IL_0104:  ldfld      uint8[] Snappy.SnappyFrame::StreamIdentifier
    IL_0109:  ldc.i4.0
    IL_010a:  ldc.i4.6
    IL_010b:  callvirt   instance void [mscorlib]System.IO.Stream::Write(uint8[],
                                                                         int32,
                                                                         int32)
    IL_0110:  ret

    IL_0111:  ldarg.0
    IL_0112:  call       instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_0117:  ldc.i4     0xfe
    IL_011c:  bne.un.s   IL_012c

    IL_011e:  ldarg.0
    IL_011f:  ldarg.1
    IL_0120:  ldarg.0
    IL_0121:  call       instance int32 Snappy.SnappyFrame::get_DataLength()
    IL_0126:  call       instance void Snappy.SnappyFrame::WriteZeroes(class [mscorlib]System.IO.Stream,
                                                                       int32)
    IL_012b:  ret

    IL_012c:  ldarg.1
    IL_012d:  ldarg.0
    IL_012e:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_0133:  ldc.i4.0
    IL_0134:  ldarg.0
    IL_0135:  ldfld      int32 Snappy.SnappyFrame::BufferUsage
    IL_013a:  callvirt   instance void [mscorlib]System.IO.Stream::Write(uint8[],
                                                                         int32,
                                                                         int32)
    IL_013f:  ret
  } // end of method SnappyFrame::Write

  .method private hidebysig instance void 
          EnsureRead(class [mscorlib]System.IO.Stream 'stream',
                     uint8[] buffer,
                     int32 offset,
                     int32 count) cil managed
  {
    // Code size       40 (0x28)
    .maxstack  4
    .locals init (int32 V_0)
    IL_0000:  br.s       IL_0022

    IL_0002:  ldarg.1
    IL_0003:  ldarg.2
    IL_0004:  ldarg.3
    IL_0005:  ldarg.s    count
    IL_0007:  callvirt   instance int32 [mscorlib]System.IO.Stream::Read(uint8[],
                                                                         int32,
                                                                         int32)
    IL_000c:  stloc.0
    IL_000d:  ldloc.0
    IL_000e:  ldc.i4.0
    IL_000f:  bgt.s      IL_0017

    IL_0011:  newobj     instance void [mscorlib]System.IO.EndOfStreamException::.ctor()
    IL_0016:  throw

    IL_0017:  ldarg.3
    IL_0018:  ldloc.0
    IL_0019:  add
    IL_001a:  starg.s    offset
    IL_001c:  ldarg.s    count
    IL_001e:  ldloc.0
    IL_001f:  sub
    IL_0020:  starg.s    count
    IL_0022:  ldarg.s    count
    IL_0024:  ldc.i4.0
    IL_0025:  bgt.s      IL_0002

    IL_0027:  ret
  } // end of method SnappyFrame::EnsureRead

  .method private hidebysig instance void 
          SkipBytes(class [mscorlib]System.IO.Stream 'stream',
                    int32 count) cil managed
  {
    // Code size       67 (0x43)
    .maxstack  5
    .locals init (int32 V_0)
    IL_0000:  ldarg.0
    IL_0001:  ldarg.2
    IL_0002:  ldc.i4     0x10000
    IL_0007:  call       int32 [mscorlib]System.Math::Min(int32,
                                                          int32)
    IL_000c:  call       instance void Snappy.SnappyFrame::EnsureBuffer(int32)
    IL_0011:  br.s       IL_003e

    IL_0013:  ldarg.1
    IL_0014:  ldarg.0
    IL_0015:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_001a:  ldc.i4.0
    IL_001b:  ldarg.2
    IL_001c:  ldarg.0
    IL_001d:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_0022:  ldlen
    IL_0023:  conv.i4
    IL_0024:  call       int32 [mscorlib]System.Math::Min(int32,
                                                          int32)
    IL_0029:  callvirt   instance int32 [mscorlib]System.IO.Stream::Read(uint8[],
                                                                         int32,
                                                                         int32)
    IL_002e:  stloc.0
    IL_002f:  ldloc.0
    IL_0030:  ldc.i4.0
    IL_0031:  bgt.s      IL_0039

    IL_0033:  newobj     instance void [mscorlib]System.IO.EndOfStreamException::.ctor()
    IL_0038:  throw

    IL_0039:  ldarg.2
    IL_003a:  ldloc.0
    IL_003b:  sub
    IL_003c:  starg.s    count
    IL_003e:  ldarg.2
    IL_003f:  ldc.i4.0
    IL_0040:  bgt.s      IL_0013

    IL_0042:  ret
  } // end of method SnappyFrame::SkipBytes

  .method private hidebysig instance void 
          WriteZeroes(class [mscorlib]System.IO.Stream 'stream',
                      int32 count) cil managed
  {
    // Code size       74 (0x4a)
    .maxstack  4
    .locals init (int32 V_0,
             int32 V_1,
             int32 V_2)
    IL_0000:  ldarg.2
    IL_0001:  ldc.i4     0x10000
    IL_0006:  call       int32 [mscorlib]System.Math::Min(int32,
                                                          int32)
    IL_000b:  stloc.0
    IL_000c:  ldarg.0
    IL_000d:  ldloc.0
    IL_000e:  call       instance void Snappy.SnappyFrame::EnsureBuffer(int32)
    IL_0013:  ldc.i4.0
    IL_0014:  stloc.1
    IL_0015:  br.s       IL_0024

    IL_0017:  ldarg.0
    IL_0018:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_001d:  ldloc.1
    IL_001e:  ldc.i4.0
    IL_001f:  stelem.i1
    IL_0020:  ldloc.1
    IL_0021:  ldc.i4.1
    IL_0022:  add
    IL_0023:  stloc.1
    IL_0024:  ldloc.1
    IL_0025:  ldloc.0
    IL_0026:  blt.s      IL_0017

    IL_0028:  br.s       IL_0045

    IL_002a:  ldarg.2
    IL_002b:  ldloc.0
    IL_002c:  call       int32 [mscorlib]System.Math::Min(int32,
                                                          int32)
    IL_0031:  stloc.2
    IL_0032:  ldarg.1
    IL_0033:  ldarg.0
    IL_0034:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_0039:  ldc.i4.0
    IL_003a:  ldloc.0
    IL_003b:  callvirt   instance void [mscorlib]System.IO.Stream::Write(uint8[],
                                                                         int32,
                                                                         int32)
    IL_0040:  ldarg.2
    IL_0041:  ldloc.2
    IL_0042:  sub
    IL_0043:  starg.s    count
    IL_0045:  ldarg.2
    IL_0046:  ldc.i4.0
    IL_0047:  bgt.s      IL_002a

    IL_0049:  ret
  } // end of method SnappyFrame::WriteZeroes

  .method private hidebysig static uint32 
          ComputeMasked(uint8[] data,
                        int32 offset,
                        int32 count) cil managed
  {
    // Code size       25 (0x19)
    .maxstack  3
    .locals init (uint32 V_0)
    IL_0000:  ldarg.0
    IL_0001:  ldarg.1
    IL_0002:  ldarg.2
    IL_0003:  call       uint32 [Crc32C.NET]Crc32C.Crc32CAlgorithm::Compute(uint8[],
                                                                            int32,
                                                                            int32)
    IL_0008:  stloc.0
    IL_0009:  ldloc.0
    IL_000a:  ldc.i4.s   15
    IL_000c:  shr.un
    IL_000d:  ldloc.0
    IL_000e:  ldc.i4.s   17
    IL_0010:  shl
    IL_0011:  or
    IL_0012:  ldc.i4     0xa282ead8
    IL_0017:  add
    IL_0018:  ret
  } // end of method SnappyFrame::ComputeMasked

  .method private hidebysig instance void 
          CheckRange(uint8[] buffer,
                     int32 offset,
                     int32 count) cil managed
  {
    // Code size       32 (0x20)
    .maxstack  8
    IL_0000:  ldarg.1
    IL_0001:  brtrue.s   IL_0009

    IL_0003:  newobj     instance void [mscorlib]System.ArgumentNullException::.ctor()
    IL_0008:  throw

    IL_0009:  ldarg.2
    IL_000a:  ldc.i4.0
    IL_000b:  blt.s      IL_0019

    IL_000d:  ldarg.3
    IL_000e:  ldc.i4.0
    IL_000f:  blt.s      IL_0019

    IL_0011:  ldarg.2
    IL_0012:  ldarg.3
    IL_0013:  add
    IL_0014:  ldarg.1
    IL_0015:  ldlen
    IL_0016:  conv.i4
    IL_0017:  ble.s      IL_001f

    IL_0019:  newobj     instance void [mscorlib]System.ArgumentOutOfRangeException::.ctor()
    IL_001e:  throw

    IL_001f:  ret
  } // end of method SnappyFrame::CheckRange

  .method private hidebysig instance void 
          CheckMaxFrameSize(int32 size) cil managed
  {
    // Code size       19 (0x13)
    .maxstack  8
    IL_0000:  ldarg.1
    IL_0001:  ldc.i4.0
    IL_0002:  blt.s      IL_000c

    IL_0004:  ldarg.1
    IL_0005:  ldc.i4     0x10000
    IL_000a:  ble.s      IL_0012

    IL_000c:  newobj     instance void [mscorlib]System.ArgumentOutOfRangeException::.ctor()
    IL_0011:  throw

    IL_0012:  ret
  } // end of method SnappyFrame::CheckMaxFrameSize

  .method private hidebysig instance void 
          EnsureBuffer(int32 size) cil managed
  {
    // Code size       32 (0x20)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_0006:  brfalse.s  IL_0013

    IL_0008:  ldarg.0
    IL_0009:  ldfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_000e:  ldlen
    IL_000f:  conv.i4
    IL_0010:  ldarg.1
    IL_0011:  bge.s      IL_001f

    IL_0013:  ldarg.0
    IL_0014:  ldarg.1
    IL_0015:  newarr     [mscorlib]System.Byte
    IL_001a:  stfld      uint8[] Snappy.SnappyFrame::Buffer
    IL_001f:  ret
  } // end of method SnappyFrame::EnsureBuffer

  .method private hidebysig specialname rtspecialname static 
          void  .cctor() cil managed
  {
    // Code size       16 (0x10)
    .maxstack  8
    IL_0000:  ldc.i4     0x10000
    IL_0005:  call       int32 Snappy.SnappyCodec::GetMaxCompressedLength(int32)
    IL_000a:  stsfld     int32 Snappy.SnappyFrame::MaxBufferUsage
    IL_000f:  ret
  } // end of method SnappyFrame::.cctor

  .property instance valuetype Snappy.SnappyFrameType
          Type()
  {
    .get instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    .set instance void Snappy.SnappyFrame::set_Type(valuetype Snappy.SnappyFrameType)
  } // end of property SnappyFrame::Type
  .property instance uint32 Checksum()
  {
    .get instance uint32 Snappy.SnappyFrame::get_Checksum()
    .set instance void Snappy.SnappyFrame::set_Checksum(uint32)
  } // end of property SnappyFrame::Checksum
  .property instance int32 DataLength()
  {
    .get instance int32 Snappy.SnappyFrame::get_DataLength()
    .set instance void Snappy.SnappyFrame::set_DataLength(int32)
  } // end of property SnappyFrame::DataLength
  .property instance uint8[] Data()
  {
    .get instance uint8[] Snappy.SnappyFrame::get_Data()
  } // end of property SnappyFrame::Data
} // end of class Snappy.SnappyFrame

.class public auto ansi sealed Snappy.SnappyFrameType
       extends [mscorlib]System.Enum
{
  .field public specialname rtspecialname uint8 value__
  .field public static literal valuetype Snappy.SnappyFrameType Compressed = uint8(0x00)
  .field public static literal valuetype Snappy.SnappyFrameType Uncompressed = uint8(0x01)
  .field public static literal valuetype Snappy.SnappyFrameType UnskippableFirst = uint8(0x02)
  .field public static literal valuetype Snappy.SnappyFrameType UnskippableLast = uint8(0x7F)
  .field public static literal valuetype Snappy.SnappyFrameType SkippableFirst = uint8(0x80)
  .field public static literal valuetype Snappy.SnappyFrameType SkippableLast = uint8(0xFD)
  .field public static literal valuetype Snappy.SnappyFrameType Padding = uint8(0xFE)
  .field public static literal valuetype Snappy.SnappyFrameType StreamIdentifier = uint8(0xFF)
} // end of class Snappy.SnappyFrameType

.class private auto ansi sealed Snappy.SnappyStatus
       extends [mscorlib]System.Enum
{
  .field public specialname rtspecialname int32 value__
  .field public static literal valuetype Snappy.SnappyStatus Ok = int32(0x00000000)
  .field public static literal valuetype Snappy.SnappyStatus InvalidInput = int32(0x00000001)
  .field public static literal valuetype Snappy.SnappyStatus BufferTooSmall = int32(0x00000002)
} // end of class Snappy.SnappyStatus

.class public auto ansi beforefieldinit Snappy.SnappyStream
       extends [mscorlib]System.IO.Stream
{
  .field private class [mscorlib]System.IO.Stream Stream
  .field private initonly valuetype [System]System.IO.Compression.CompressionMode Mode
  .field private initonly bool LeaveOpen
  .field private class Snappy.SnappyFrame Frame
  .field private uint8[] Buffer
  .field private int32 BufferUsage
  .field private int32 BufferRead
  .field private bool InitializedStream
  .field private bool BadStream
  .method public hidebysig specialname virtual 
          instance bool  get_CanRead() cil managed
  {
    // Code size       30 (0x1e)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldfld      class [mscorlib]System.IO.Stream Snappy.SnappyStream::Stream
    IL_0006:  brfalse.s  IL_001c

    IL_0008:  ldarg.0
    IL_0009:  ldfld      valuetype [System]System.IO.Compression.CompressionMode Snappy.SnappyStream::Mode
    IL_000e:  brtrue.s   IL_001c

    IL_0010:  ldarg.0
    IL_0011:  ldfld      class [mscorlib]System.IO.Stream Snappy.SnappyStream::Stream
    IL_0016:  callvirt   instance bool [mscorlib]System.IO.Stream::get_CanRead()
    IL_001b:  ret

    IL_001c:  ldc.i4.0
    IL_001d:  ret
  } // end of method SnappyStream::get_CanRead

  .method public hidebysig specialname virtual 
          instance bool  get_CanWrite() cil managed
  {
    // Code size       31 (0x1f)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldfld      class [mscorlib]System.IO.Stream Snappy.SnappyStream::Stream
    IL_0006:  brfalse.s  IL_001d

    IL_0008:  ldarg.0
    IL_0009:  ldfld      valuetype [System]System.IO.Compression.CompressionMode Snappy.SnappyStream::Mode
    IL_000e:  ldc.i4.1
    IL_000f:  bne.un.s   IL_001d

    IL_0011:  ldarg.0
    IL_0012:  ldfld      class [mscorlib]System.IO.Stream Snappy.SnappyStream::Stream
    IL_0017:  callvirt   instance bool [mscorlib]System.IO.Stream::get_CanWrite()
    IL_001c:  ret

    IL_001d:  ldc.i4.0
    IL_001e:  ret
  } // end of method SnappyStream::get_CanWrite

  .method public hidebysig specialname virtual 
          instance bool  get_CanSeek() cil managed
  {
    // Code size       2 (0x2)
    .maxstack  8
    IL_0000:  ldc.i4.0
    IL_0001:  ret
  } // end of method SnappyStream::get_CanSeek

  .method public hidebysig specialname virtual 
          instance int64  get_Length() cil managed
  {
    // Code size       6 (0x6)
    .maxstack  8
    IL_0000:  newobj     instance void [mscorlib]System.NotSupportedException::.ctor()
    IL_0005:  throw
  } // end of method SnappyStream::get_Length

  .method public hidebysig specialname virtual 
          instance int64  get_Position() cil managed
  {
    // Code size       6 (0x6)
    .maxstack  8
    IL_0000:  newobj     instance void [mscorlib]System.NotSupportedException::.ctor()
    IL_0005:  throw
  } // end of method SnappyStream::get_Position

  .method public hidebysig specialname virtual 
          instance void  set_Position(int64 'value') cil managed
  {
    // Code size       6 (0x6)
    .maxstack  8
    IL_0000:  newobj     instance void [mscorlib]System.NotSupportedException::.ctor()
    IL_0005:  throw
  } // end of method SnappyStream::set_Position

  .method public hidebysig specialname rtspecialname 
          instance void  .ctor(class [mscorlib]System.IO.Stream 'stream',
                               valuetype [System]System.IO.Compression.CompressionMode mode) cil managed
  {
    // Code size       10 (0xa)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldarg.1
    IL_0002:  ldarg.2
    IL_0003:  ldc.i4.0
    IL_0004:  call       instance void Snappy.SnappyStream::.ctor(class [mscorlib]System.IO.Stream,
                                                                  valuetype [System]System.IO.Compression.CompressionMode,
                                                                  bool)
    IL_0009:  ret
  } // end of method SnappyStream::.ctor

  .method public hidebysig specialname rtspecialname 
          instance void  .ctor(class [mscorlib]System.IO.Stream 'stream',
                               valuetype [System]System.IO.Compression.CompressionMode mode,
                               bool leaveOpen) cil managed
  {
    // Code size       55 (0x37)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  newobj     instance void Snappy.SnappyFrame::.ctor()
    IL_0006:  stfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
    IL_000b:  ldarg.0
    IL_000c:  ldc.i4     0x100
    IL_0011:  newarr     [mscorlib]System.Byte
    IL_0016:  stfld      uint8[] Snappy.SnappyStream::Buffer
    IL_001b:  ldarg.0
    IL_001c:  call       instance void [mscorlib]System.IO.Stream::.ctor()
    IL_0021:  ldarg.0
    IL_0022:  ldarg.1
    IL_0023:  stfld      class [mscorlib]System.IO.Stream Snappy.SnappyStream::Stream
    IL_0028:  ldarg.0
    IL_0029:  ldarg.2
    IL_002a:  stfld      valuetype [System]System.IO.Compression.CompressionMode Snappy.SnappyStream::Mode
    IL_002f:  ldarg.0
    IL_0030:  ldarg.3
    IL_0031:  stfld      bool Snappy.SnappyStream::LeaveOpen
    IL_0036:  ret
  } // end of method SnappyStream::.ctor

  .method family hidebysig virtual instance void 
          Dispose(bool disposing) cil managed
  {
    // Code size       54 (0x36)
    .maxstack  2
    .try
    {
      IL_0000:  ldarg.0
      IL_0001:  ldfld      valuetype [System]System.IO.Compression.CompressionMode Snappy.SnappyStream::Mode
      IL_0006:  ldc.i4.1
      IL_0007:  bne.un.s   IL_0012

      IL_0009:  ldarg.1
      IL_000a:  brfalse.s  IL_0012

      IL_000c:  ldarg.0
      IL_000d:  callvirt   instance void [mscorlib]System.IO.Stream::Flush()
      IL_0012:  leave.s    IL_0035

    }  // end .try
    finally
    {
      .try
      {
        IL_0014:  ldarg.0
        IL_0015:  ldfld      bool Snappy.SnappyStream::LeaveOpen
        IL_001a:  brtrue.s   IL_002a

        IL_001c:  ldarg.1
        IL_001d:  brfalse.s  IL_002a

        IL_001f:  ldarg.0
        IL_0020:  ldfld      class [mscorlib]System.IO.Stream Snappy.SnappyStream::Stream
        IL_0025:  callvirt   instance void [mscorlib]System.IO.Stream::Close()
        IL_002a:  leave.s    IL_0034

      }  // end .try
      finally
      {
        IL_002c:  ldarg.0
        IL_002d:  ldnull
        IL_002e:  stfld      class [mscorlib]System.IO.Stream Snappy.SnappyStream::Stream
        IL_0033:  endfinally
      }  // end handler
      IL_0034:  endfinally
    }  // end handler
    IL_0035:  ret
  } // end of method SnappyStream::Dispose

  .method public hidebysig virtual instance int32 
          Read(uint8[] buffer,
               int32 offset,
               int32 count) cil managed
  {
    // Code size       125 (0x7d)
    .maxstack  5
    .locals init (int32 V_0,
             int32 V_1,
             int32 V_2)
    .try
    {
      IL_0000:  ldarg.0
      IL_0001:  call       instance void Snappy.SnappyStream::EnsureDecompressionMode()
      IL_0006:  ldarg.0
      IL_0007:  ldarg.1
      IL_0008:  ldarg.2
      IL_0009:  ldarg.3
      IL_000a:  call       instance void Snappy.SnappyStream::ValidateRange(uint8[],
                                                                            int32,
                                                                            int32)
      IL_000f:  ldarg.0
      IL_0010:  call       instance void Snappy.SnappyStream::InitializeStream()
      IL_0015:  ldc.i4.0
      IL_0016:  stloc.0
      IL_0017:  br.s       IL_0069

      IL_0019:  ldarg.0
      IL_001a:  call       instance bool Snappy.SnappyStream::EnsureAvailable()
      IL_001f:  brtrue.s   IL_0025

      IL_0021:  ldloc.0
      IL_0022:  stloc.2
      IL_0023:  leave.s    IL_007b

      IL_0025:  ldarg.3
      IL_0026:  ldarg.0
      IL_0027:  ldfld      int32 Snappy.SnappyStream::BufferUsage
      IL_002c:  ldarg.0
      IL_002d:  ldfld      int32 Snappy.SnappyStream::BufferRead
      IL_0032:  sub
      IL_0033:  call       int32 [mscorlib]System.Math::Min(int32,
                                                            int32)
      IL_0038:  stloc.1
      IL_0039:  ldarg.0
      IL_003a:  ldfld      uint8[] Snappy.SnappyStream::Buffer
      IL_003f:  ldarg.0
      IL_0040:  ldfld      int32 Snappy.SnappyStream::BufferRead
      IL_0045:  ldarg.1
      IL_0046:  ldarg.2
      IL_0047:  ldloc.1
      IL_0048:  call       void [mscorlib]System.Array::Copy(class [mscorlib]System.Array,
                                                             int32,
                                                             class [mscorlib]System.Array,
                                                             int32,
                                                             int32)
      IL_004d:  ldloc.0
      IL_004e:  ldloc.1
      IL_004f:  add
      IL_0050:  stloc.0
      IL_0051:  ldarg.2
      IL_0052:  ldloc.1
      IL_0053:  add
      IL_0054:  starg.s    offset
      IL_0056:  ldarg.3
      IL_0057:  ldloc.1
      IL_0058:  sub
      IL_0059:  starg.s    count
      IL_005b:  ldarg.0
      IL_005c:  dup
      IL_005d:  ldfld      int32 Snappy.SnappyStream::BufferRead
      IL_0062:  ldloc.1
      IL_0063:  add
      IL_0064:  stfld      int32 Snappy.SnappyStream::BufferRead
      IL_0069:  ldarg.3
      IL_006a:  ldc.i4.0
      IL_006b:  bgt.s      IL_0019

      IL_006d:  ldloc.0
      IL_006e:  stloc.2
      IL_006f:  leave.s    IL_007b

    }  // end .try
    catch [mscorlib]System.Object 
    {
      IL_0071:  pop
      IL_0072:  ldarg.0
      IL_0073:  ldc.i4.1
      IL_0074:  stfld      bool Snappy.SnappyStream::BadStream
      IL_0079:  rethrow
    }  // end handler
    IL_007b:  ldloc.2
    IL_007c:  ret
  } // end of method SnappyStream::Read

  .method public hidebysig virtual instance int32 
          ReadByte() cil managed
  {
    // Code size       68 (0x44)
    .maxstack  3
    .locals init (uint8 V_0,
             int32 V_1)
    .try
    {
      IL_0000:  ldarg.0
      IL_0001:  call       instance void Snappy.SnappyStream::EnsureDecompressionMode()
      IL_0006:  ldarg.0
      IL_0007:  call       instance void Snappy.SnappyStream::InitializeStream()
      IL_000c:  ldarg.0
      IL_000d:  call       instance bool Snappy.SnappyStream::EnsureAvailable()
      IL_0012:  brtrue.s   IL_0018

      IL_0014:  ldc.i4.m1
      IL_0015:  stloc.1
      IL_0016:  leave.s    IL_0042

      IL_0018:  ldarg.0
      IL_0019:  ldfld      uint8[] Snappy.SnappyStream::Buffer
      IL_001e:  ldarg.0
      IL_001f:  ldfld      int32 Snappy.SnappyStream::BufferRead
      IL_0024:  ldelem.u1
      IL_0025:  stloc.0
      IL_0026:  ldarg.0
      IL_0027:  dup
      IL_0028:  ldfld      int32 Snappy.SnappyStream::BufferRead
      IL_002d:  ldc.i4.1
      IL_002e:  add
      IL_002f:  stfld      int32 Snappy.SnappyStream::BufferRead
      IL_0034:  ldloc.0
      IL_0035:  stloc.1
      IL_0036:  leave.s    IL_0042

    }  // end .try
    catch [mscorlib]System.Object 
    {
      IL_0038:  pop
      IL_0039:  ldarg.0
      IL_003a:  ldc.i4.1
      IL_003b:  stfld      bool Snappy.SnappyStream::BadStream
      IL_0040:  rethrow
    }  // end handler
    IL_0042:  ldloc.1
    IL_0043:  ret
  } // end of method SnappyStream::ReadByte

  .method public hidebysig virtual instance void 
          Write(uint8[] buffer,
                int32 offset,
                int32 count) cil managed
  {
    // Code size       136 (0x88)
    .maxstack  5
    .locals init (int32 V_0)
    .try
    {
      IL_0000:  ldarg.0
      IL_0001:  call       instance void Snappy.SnappyStream::EnsureCompressionMode()
      IL_0006:  ldarg.0
      IL_0007:  ldarg.1
      IL_0008:  ldarg.2
      IL_0009:  ldarg.3
      IL_000a:  call       instance void Snappy.SnappyStream::ValidateRange(uint8[],
                                                                            int32,
                                                                            int32)
      IL_000f:  ldarg.0
      IL_0010:  call       instance void Snappy.SnappyStream::InitializeStream()
      IL_0015:  br.s       IL_0077

      IL_0017:  ldarg.3
      IL_0018:  ldc.i4     0x10000
      IL_001d:  ldarg.0
      IL_001e:  ldfld      int32 Snappy.SnappyStream::BufferUsage
      IL_0023:  sub
      IL_0024:  call       int32 [mscorlib]System.Math::Min(int32,
                                                            int32)
      IL_0029:  stloc.0
      IL_002a:  ldarg.0
      IL_002b:  ldarg.0
      IL_002c:  ldfld      int32 Snappy.SnappyStream::BufferUsage
      IL_0031:  ldloc.0
      IL_0032:  add
      IL_0033:  call       instance void Snappy.SnappyStream::EnsureBuffer(int32)
      IL_0038:  ldarg.1
      IL_0039:  ldarg.2
      IL_003a:  ldarg.0
      IL_003b:  ldfld      uint8[] Snappy.SnappyStream::Buffer
      IL_0040:  ldarg.0
      IL_0041:  ldfld      int32 Snappy.SnappyStream::BufferUsage
      IL_0046:  ldloc.0
      IL_0047:  call       void [mscorlib]System.Array::Copy(class [mscorlib]System.Array,
                                                             int32,
                                                             class [mscorlib]System.Array,
                                                             int32,
                                                             int32)
      IL_004c:  ldarg.2
      IL_004d:  ldloc.0
      IL_004e:  add
      IL_004f:  starg.s    offset
      IL_0051:  ldarg.3
      IL_0052:  ldloc.0
      IL_0053:  sub
      IL_0054:  starg.s    count
      IL_0056:  ldarg.0
      IL_0057:  dup
      IL_0058:  ldfld      int32 Snappy.SnappyStream::BufferUsage
      IL_005d:  ldloc.0
      IL_005e:  add
      IL_005f:  stfld      int32 Snappy.SnappyStream::BufferUsage
      IL_0064:  ldarg.0
      IL_0065:  ldfld      int32 Snappy.SnappyStream::BufferUsage
      IL_006a:  ldc.i4     0x10000
      IL_006f:  bne.un.s   IL_0077

      IL_0071:  ldarg.0
      IL_0072:  callvirt   instance void [mscorlib]System.IO.Stream::Flush()
      IL_0077:  ldarg.3
      IL_0078:  ldc.i4.0
      IL_0079:  bgt.s      IL_0017

      IL_007b:  leave.s    IL_0087

    }  // end .try
    catch [mscorlib]System.Object 
    {
      IL_007d:  pop
      IL_007e:  ldarg.0
      IL_007f:  ldc.i4.1
      IL_0080:  stfld      bool Snappy.SnappyStream::BadStream
      IL_0085:  rethrow
    }  // end handler
    IL_0087:  ret
  } // end of method SnappyStream::Write

  .method public hidebysig virtual instance void 
          WriteByte(uint8 'value') cil managed
  {
    // Code size       86 (0x56)
    .maxstack  3
    .try
    {
      IL_0000:  ldarg.0
      IL_0001:  call       instance void Snappy.SnappyStream::EnsureCompressionMode()
      IL_0006:  ldarg.0
      IL_0007:  call       instance void Snappy.SnappyStream::InitializeStream()
      IL_000c:  ldarg.0
      IL_000d:  ldarg.0
      IL_000e:  ldfld      int32 Snappy.SnappyStream::BufferUsage
      IL_0013:  ldc.i4.1
      IL_0014:  add
      IL_0015:  call       instance void Snappy.SnappyStream::EnsureBuffer(int32)
      IL_001a:  ldarg.0
      IL_001b:  ldfld      uint8[] Snappy.SnappyStream::Buffer
      IL_0020:  ldarg.0
      IL_0021:  ldfld      int32 Snappy.SnappyStream::BufferUsage
      IL_0026:  ldarg.1
      IL_0027:  stelem.i1
      IL_0028:  ldarg.0
      IL_0029:  dup
      IL_002a:  ldfld      int32 Snappy.SnappyStream::BufferUsage
      IL_002f:  ldc.i4.1
      IL_0030:  add
      IL_0031:  stfld      int32 Snappy.SnappyStream::BufferUsage
      IL_0036:  ldarg.0
      IL_0037:  ldfld      int32 Snappy.SnappyStream::BufferUsage
      IL_003c:  ldc.i4     0x10000
      IL_0041:  bne.un.s   IL_0049

      IL_0043:  ldarg.0
      IL_0044:  callvirt   instance void [mscorlib]System.IO.Stream::Flush()
      IL_0049:  leave.s    IL_0055

    }  // end .try
    catch [mscorlib]System.Object 
    {
      IL_004b:  pop
      IL_004c:  ldarg.0
      IL_004d:  ldc.i4.1
      IL_004e:  stfld      bool Snappy.SnappyStream::BadStream
      IL_0053:  rethrow
    }  // end handler
    IL_0055:  ret
  } // end of method SnappyStream::WriteByte

  .method public hidebysig virtual instance void 
          Flush() cil managed
  {
    // Code size       82 (0x52)
    .maxstack  4
    .try
    {
      IL_0000:  ldarg.0
      IL_0001:  call       instance void Snappy.SnappyStream::EnsureCompressionMode()
      IL_0006:  ldarg.0
      IL_0007:  call       instance void Snappy.SnappyStream::InitializeStream()
      IL_000c:  ldarg.0
      IL_000d:  ldfld      int32 Snappy.SnappyStream::BufferUsage
      IL_0012:  ldc.i4.0
      IL_0013:  ble.s      IL_0045

      IL_0015:  ldarg.0
      IL_0016:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
      IL_001b:  ldarg.0
      IL_001c:  ldfld      uint8[] Snappy.SnappyStream::Buffer
      IL_0021:  ldc.i4.0
      IL_0022:  ldarg.0
      IL_0023:  ldfld      int32 Snappy.SnappyStream::BufferUsage
      IL_0028:  callvirt   instance void Snappy.SnappyFrame::SetCompressed(uint8[],
                                                                           int32,
                                                                           int32)
      IL_002d:  ldarg.0
      IL_002e:  ldc.i4.0
      IL_002f:  stfld      int32 Snappy.SnappyStream::BufferUsage
      IL_0034:  ldarg.0
      IL_0035:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
      IL_003a:  ldarg.0
      IL_003b:  ldfld      class [mscorlib]System.IO.Stream Snappy.SnappyStream::Stream
      IL_0040:  callvirt   instance void Snappy.SnappyFrame::Write(class [mscorlib]System.IO.Stream)
      IL_0045:  leave.s    IL_0051

    }  // end .try
    catch [mscorlib]System.Object 
    {
      IL_0047:  pop
      IL_0048:  ldarg.0
      IL_0049:  ldc.i4.1
      IL_004a:  stfld      bool Snappy.SnappyStream::BadStream
      IL_004f:  rethrow
    }  // end handler
    IL_0051:  ret
  } // end of method SnappyStream::Flush

  .method public hidebysig virtual instance void 
          SetLength(int64 'value') cil managed
  {
    // Code size       6 (0x6)
    .maxstack  8
    IL_0000:  newobj     instance void [mscorlib]System.NotSupportedException::.ctor()
    IL_0005:  throw
  } // end of method SnappyStream::SetLength

  .method public hidebysig virtual instance int64 
          Seek(int64 offset,
               valuetype [mscorlib]System.IO.SeekOrigin origin) cil managed
  {
    // Code size       6 (0x6)
    .maxstack  8
    IL_0000:  newobj     instance void [mscorlib]System.NotSupportedException::.ctor()
    IL_0005:  throw
  } // end of method SnappyStream::Seek

  .method private hidebysig instance void 
          InitializeStream() cil managed
  {
    // Code size       104 (0x68)
    .maxstack  2
    IL_0000:  ldarg.0
    IL_0001:  ldfld      bool Snappy.SnappyStream::InitializedStream
    IL_0006:  brtrue.s   IL_0067

    IL_0008:  ldarg.0
    IL_0009:  ldfld      valuetype [System]System.IO.Compression.CompressionMode Snappy.SnappyStream::Mode
    IL_000e:  ldc.i4.1
    IL_000f:  bne.un.s   IL_002f

    IL_0011:  ldarg.0
    IL_0012:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
    IL_0017:  callvirt   instance void Snappy.SnappyFrame::SetStreamIdentifier()
    IL_001c:  ldarg.0
    IL_001d:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
    IL_0022:  ldarg.0
    IL_0023:  ldfld      class [mscorlib]System.IO.Stream Snappy.SnappyStream::Stream
    IL_0028:  callvirt   instance void Snappy.SnappyFrame::Write(class [mscorlib]System.IO.Stream)
    IL_002d:  br.s       IL_0060

    IL_002f:  ldarg.0
    IL_0030:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
    IL_0035:  ldarg.0
    IL_0036:  ldfld      class [mscorlib]System.IO.Stream Snappy.SnappyStream::Stream
    IL_003b:  callvirt   instance bool Snappy.SnappyFrame::Read(class [mscorlib]System.IO.Stream)
    IL_0040:  brtrue.s   IL_0048

    IL_0042:  newobj     instance void [mscorlib]System.IO.EndOfStreamException::.ctor()
    IL_0047:  throw

    IL_0048:  ldarg.0
    IL_0049:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
    IL_004e:  callvirt   instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_0053:  ldc.i4     0xff
    IL_0058:  beq.s      IL_0060

    IL_005a:  newobj     instance void [System]System.IO.InvalidDataException::.ctor()
    IL_005f:  throw

    IL_0060:  ldarg.0
    IL_0061:  ldc.i4.1
    IL_0062:  stfld      bool Snappy.SnappyStream::InitializedStream
    IL_0067:  ret
  } // end of method SnappyStream::InitializeStream

  .method private hidebysig instance bool 
          EnsureAvailable() cil managed
  {
    // Code size       135 (0x87)
    .maxstack  2
    IL_0000:  ldarg.0
    IL_0001:  ldfld      int32 Snappy.SnappyStream::BufferRead
    IL_0006:  ldarg.0
    IL_0007:  ldfld      int32 Snappy.SnappyStream::BufferUsage
    IL_000c:  blt.s      IL_0085

    IL_000e:  ldarg.0
    IL_000f:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
    IL_0014:  ldarg.0
    IL_0015:  ldfld      class [mscorlib]System.IO.Stream Snappy.SnappyStream::Stream
    IL_001a:  callvirt   instance bool Snappy.SnappyFrame::Read(class [mscorlib]System.IO.Stream)
    IL_001f:  brtrue.s   IL_0023

    IL_0021:  ldc.i4.0
    IL_0022:  ret

    IL_0023:  ldarg.0
    IL_0024:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
    IL_0029:  callvirt   instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_002e:  brfalse.s  IL_003e

    IL_0030:  ldarg.0
    IL_0031:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
    IL_0036:  callvirt   instance valuetype Snappy.SnappyFrameType Snappy.SnappyFrame::get_Type()
    IL_003b:  ldc.i4.1
    IL_003c:  bne.un.s   IL_000e

    IL_003e:  ldarg.0
    IL_003f:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
    IL_0044:  callvirt   instance int32 Snappy.SnappyFrame::get_DataLength()
    IL_0049:  brfalse.s  IL_000e

    IL_004b:  ldarg.0
    IL_004c:  ldarg.0
    IL_004d:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
    IL_0052:  callvirt   instance int32 Snappy.SnappyFrame::get_DataLength()
    IL_0057:  call       instance void Snappy.SnappyStream::EnsureBuffer(int32)
    IL_005c:  ldarg.0
    IL_005d:  ldc.i4.0
    IL_005e:  stfld      int32 Snappy.SnappyStream::BufferRead
    IL_0063:  ldarg.0
    IL_0064:  ldarg.0
    IL_0065:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
    IL_006a:  callvirt   instance int32 Snappy.SnappyFrame::get_DataLength()
    IL_006f:  stfld      int32 Snappy.SnappyStream::BufferUsage
    IL_0074:  ldarg.0
    IL_0075:  ldfld      class Snappy.SnappyFrame Snappy.SnappyStream::Frame
    IL_007a:  ldarg.0
    IL_007b:  ldfld      uint8[] Snappy.SnappyStream::Buffer
    IL_0080:  callvirt   instance void Snappy.SnappyFrame::GetData(uint8[])
    IL_0085:  ldc.i4.1
    IL_0086:  ret
  } // end of method SnappyStream::EnsureAvailable

  .method private hidebysig instance void 
          EnsureCompressionMode() cil managed
  {
    // Code size       27 (0x1b)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  call       instance void Snappy.SnappyStream::CheckStream()
    IL_0006:  ldarg.0
    IL_0007:  ldfld      valuetype [System]System.IO.Compression.CompressionMode Snappy.SnappyStream::Mode
    IL_000c:  ldc.i4.1
    IL_000d:  beq.s      IL_001a

    IL_000f:  ldstr      "Use read operations on decompression stream"
    IL_0014:  newobj     instance void [mscorlib]System.InvalidOperationException::.ctor(string)
    IL_0019:  throw

    IL_001a:  ret
  } // end of method SnappyStream::EnsureCompressionMode

  .method private hidebysig instance void 
          EnsureDecompressionMode() cil managed
  {
    // Code size       26 (0x1a)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  call       instance void Snappy.SnappyStream::CheckStream()
    IL_0006:  ldarg.0
    IL_0007:  ldfld      valuetype [System]System.IO.Compression.CompressionMode Snappy.SnappyStream::Mode
    IL_000c:  brfalse.s  IL_0019

    IL_000e:  ldstr      "Use write operations on compression stream"
    IL_0013:  newobj     instance void [mscorlib]System.InvalidOperationException::.ctor(string)
    IL_0018:  throw

    IL_0019:  ret
  } // end of method SnappyStream::EnsureDecompressionMode

  .method private hidebysig instance void 
          CheckStream() cil managed
  {
    // Code size       39 (0x27)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldfld      class [mscorlib]System.IO.Stream Snappy.SnappyStream::Stream
    IL_0006:  brtrue.s   IL_0013

    IL_0008:  ldstr      "SnappyStream"
    IL_000d:  newobj     instance void [mscorlib]System.ObjectDisposedException::.ctor(string)
    IL_0012:  throw

    IL_0013:  ldarg.0
    IL_0014:  ldfld      bool Snappy.SnappyStream::BadStream
    IL_0019:  brfalse.s  IL_0026

    IL_001b:  ldstr      "SnappyStream is in broken state due to previous er"
    + "ror"
    IL_0020:  newobj     instance void [mscorlib]System.InvalidOperationException::.ctor(string)
    IL_0025:  throw

    IL_0026:  ret
  } // end of method SnappyStream::CheckStream

  .method private hidebysig instance void 
          ValidateRange(uint8[] buffer,
                        int32 offset,
                        int32 count) cil managed
  {
    // Code size       32 (0x20)
    .maxstack  8
    IL_0000:  ldarg.1
    IL_0001:  brtrue.s   IL_0009

    IL_0003:  newobj     instance void [mscorlib]System.ArgumentNullException::.ctor()
    IL_0008:  throw

    IL_0009:  ldarg.2
    IL_000a:  ldc.i4.0
    IL_000b:  blt.s      IL_0019

    IL_000d:  ldarg.3
    IL_000e:  ldc.i4.0
    IL_000f:  blt.s      IL_0019

    IL_0011:  ldarg.2
    IL_0012:  ldarg.3
    IL_0013:  add
    IL_0014:  ldarg.1
    IL_0015:  ldlen
    IL_0016:  conv.i4
    IL_0017:  ble.s      IL_001f

    IL_0019:  newobj     instance void [mscorlib]System.ArgumentOutOfRangeException::.ctor()
    IL_001e:  throw

    IL_001f:  ret
  } // end of method SnappyStream::ValidateRange

  .method private hidebysig instance void 
          EnsureBuffer(int32 size) cil managed
  {
    // Code size       67 (0x43)
    .maxstack  5
    .locals init (int32 V_0,
             uint8[] V_1)
    IL_0000:  ldarg.1
    IL_0001:  ldarg.0
    IL_0002:  ldfld      uint8[] Snappy.SnappyStream::Buffer
    IL_0007:  ldlen
    IL_0008:  conv.i4
    IL_0009:  ble.s      IL_0042

    IL_000b:  ldc.i4.2
    IL_000c:  ldarg.0
    IL_000d:  ldfld      uint8[] Snappy.SnappyStream::Buffer
    IL_0012:  ldlen
    IL_0013:  conv.i4
    IL_0014:  mul
    IL_0015:  stloc.0
    IL_0016:  br.s       IL_001c

    IL_0018:  ldloc.0
    IL_0019:  ldc.i4.2
    IL_001a:  mul
    IL_001b:  stloc.0
    IL_001c:  ldloc.0
    IL_001d:  ldarg.1
    IL_001e:  blt.s      IL_0018

    IL_0020:  ldloc.0
    IL_0021:  newarr     [mscorlib]System.Byte
    IL_0026:  stloc.1
    IL_0027:  ldarg.0
    IL_0028:  ldfld      uint8[] Snappy.SnappyStream::Buffer
    IL_002d:  ldc.i4.0
    IL_002e:  ldloc.1
    IL_002f:  ldc.i4.0
    IL_0030:  ldarg.0
    IL_0031:  ldfld      int32 Snappy.SnappyStream::BufferUsage
    IL_0036:  call       void [mscorlib]System.Array::Copy(class [mscorlib]System.Array,
                                                           int32,
                                                           class [mscorlib]System.Array,
                                                           int32,
                                                           int32)
    IL_003b:  ldarg.0
    IL_003c:  ldloc.1
    IL_003d:  stfld      uint8[] Snappy.SnappyStream::Buffer
    IL_0042:  ret
  } // end of method SnappyStream::EnsureBuffer

  .property instance bool CanRead()
  {
    .get instance bool Snappy.SnappyStream::get_CanRead()
  } // end of property SnappyStream::CanRead
  .property instance bool CanWrite()
  {
    .get instance bool Snappy.SnappyStream::get_CanWrite()
  } // end of property SnappyStream::CanWrite
  .property instance bool CanSeek()
  {
    .get instance bool Snappy.SnappyStream::get_CanSeek()
  } // end of property SnappyStream::CanSeek
  .property instance int64 Length()
  {
    .get instance int64 Snappy.SnappyStream::get_Length()
  } // end of property SnappyStream::Length
  .property instance int64 Position()
  {
    .get instance int64 Snappy.SnappyStream::get_Position()
    .set instance void Snappy.SnappyStream::set_Position(int64)
  } // end of property SnappyStream::Position
} // end of class Snappy.SnappyStream

.class private auto ansi beforefieldinit Snappy.Utils
       extends [mscorlib]System.Object
{
  .method public hidebysig static bool  BuffersEqual(uint8[] left,
                                                     uint8[] right) cil managed
  {
    // Code size       21 (0x15)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldlen
    IL_0002:  conv.i4
    IL_0003:  ldarg.1
    IL_0004:  ldlen
    IL_0005:  conv.i4
    IL_0006:  bne.un.s   IL_0013

    IL_0008:  ldarg.0
    IL_0009:  ldarg.1
    IL_000a:  ldarg.0
    IL_000b:  ldlen
    IL_000c:  conv.i4
    IL_000d:  call       bool Snappy.Utils::BuffersEqual(uint8[],
                                                         uint8[],
                                                         int32)
    IL_0012:  ret

    IL_0013:  ldc.i4.0
    IL_0014:  ret
  } // end of method Utils::BuffersEqual

  .method public hidebysig static bool  BuffersEqual(uint8[] left,
                                                     uint8[] right,
                                                     int32 count) cil managed
  {
    // Code size       24 (0x18)
    .maxstack  3
    .locals init (int32 V_0)
    IL_0000:  ldc.i4.0
    IL_0001:  stloc.0
    IL_0002:  br.s       IL_0012

    IL_0004:  ldarg.0
    IL_0005:  ldloc.0
    IL_0006:  ldelem.u1
    IL_0007:  ldarg.1
    IL_0008:  ldloc.0
    IL_0009:  ldelem.u1
    IL_000a:  beq.s      IL_000e

    IL_000c:  ldc.i4.0
    IL_000d:  ret

    IL_000e:  ldloc.0
    IL_000f:  ldc.i4.1
    IL_0010:  add
    IL_0011:  stloc.0
    IL_0012:  ldloc.0
    IL_0013:  ldarg.2
    IL_0014:  blt.s      IL_0004

    IL_0016:  ldc.i4.1
    IL_0017:  ret
  } // end of method Utils::BuffersEqual

  .method public hidebysig specialname rtspecialname 
          instance void  .ctor() cil managed
  {
    // Code size       7 (0x7)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  call       instance void [mscorlib]System.Object::.ctor()
    IL_0006:  ret
  } // end of method Utils::.ctor

} // end of class Snappy.Utils


// =============================================================

// *********** DISASSEMBLY COMPLETE ***********************
// WARNING: Created Win32 resource file src\References\Snappy.NET.res
