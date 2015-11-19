<#
Creates Leiningen [checkout dependency][checkout-deps] links between the
sub-projects to provide for a more streamlined REPL development
experience.

Just run this from the root project directory (the directory containing
this script) before firing up a REPL.

[checkout-deps]: https://github.com/technomancy/leiningen/blob/stable/doc/TUTORIAL.md#checkout-dependencies
#>

Param(
  [string]$rootDir = $pwd
)

$windowsId = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$principal = new-object System.Security.Principal.WindowsPrincipal($windowsID)
$adminRole = [System.Security.Principal.WindowsBuiltInRole]::Administrator

[string[]]$argList = @(
  '-NoProfile',
  '-NoExit',
  '-File',
  $MyInvocation.MyCommand.Path,
  $rootDir
)

If (-NOT $principal.IsInRole($adminRole)) {
    Start-Process powershell.exe -Verb RunAs -ArgumentList $argList
    Break
}


Try {
    $null = [mklink.symlink]
} Catch {
    Add-Type @"
    using System;
    using System.Runtime.InteropServices;

    namespace mklink
    {
        public class symlink
        {
            [DllImport("kernel32.dll")]
            public static extern bool CreateSymbolicLink(
              string lpSymlinkFileName,
              string lpTargetFileName,
              int dwFlags
            );
        }
    }
"@
}

$cdcUtil = (Resolve-Path (Join-Path $rootDir .\cdc-util)).path
$cdcInit = (Resolve-Path (Join-Path $rootDir .\cdc-init)).path

If (-NOT (Test-Path ([io.path]::Combine($cdcInit, 'checkouts')))) {
  New-Item -Path $cdcInit -Name "checkouts" -ItemType "directory"
}

[mklink.symlink]::CreateSymbolicLink(
  [io.path]::Combine($cdcInit, 'checkouts\cdc-util'),
  $cdcUtil,
  1
)

$cdcPub = (Resolve-Path (Join-Path $rootDir .\cdc-publisher)).path

If (-NOT (Test-Path ([io.path]::Combine($cdcPub, 'checkouts')))) {
  New-Item -Path $cdcPub -Name "checkouts" -ItemType "directory"
}

[mklink.symlink]::CreateSymbolicLink(
  [io.path]::Combine($cdcPub, 'checkouts\cdc-util'),
  $cdcUtil,
  1
)
