{
  description = "Nix flake for AthenaCLI and RedshiftCLI";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      lib = nixpkgs.lib;
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      unsupportedSystemMessage =
        "Unsupported system. This flake supports Linux and macOS only.";
      _guard = if builtins ? currentSystem
        && !(lib.elem builtins.currentSystem supportedSystems)
        then builtins.throw unsupportedSystemMessage
        else null;
      forAllSystems = f: lib.genAttrs supportedSystems (system: f system);
    in {
      packages = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          pythonPackages = pkgs.python3Packages;
          runtimeDeps = with pythonPackages; [
            click
            pygments
            prompt-toolkit
            sqlparse
            configobj
            cli-helpers
            botocore
            boto3
            pyathena
          ];
          redshiftDeps = with pythonPackages; [
            psycopg2-binary
          ];
        in {
          athenacli = pythonPackages.buildPythonApplication {
            pname = "athenacli";
            version = "0.0.0";
            src = ./.;
            format = "setuptools";
            nativeBuildInputs = [
              pythonPackages.setuptools
              pythonPackages.wheel
            ];
            propagatedBuildInputs = runtimeDeps;
            pythonImportsCheck = [ "athenacli" ];
          };

          redshiftcli = pythonPackages.buildPythonApplication {
            pname = "redshiftcli";
            version = "0.0.0";
            src = ./.;
            format = "setuptools";
            nativeBuildInputs = [
              pythonPackages.setuptools
              pythonPackages.wheel
            ];
            propagatedBuildInputs = runtimeDeps ++ redshiftDeps;
            pythonImportsCheck = [ "redshiftcli" ];
          };

          default = self.packages.${system}.athenacli;
        });

      apps = forAllSystems (system: {
        athenacli = {
          type = "app";
          program = "${self.packages.${system}.athenacli}/bin/athenacli";
        };
        redshiftcli = {
          type = "app";
          program = "${self.packages.${system}.redshiftcli}/bin/redshiftcli";
        };
        default = self.apps.${system}.athenacli;
      });

      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          pythonPackages = pkgs.python3Packages;
          devDeps = with pythonPackages; [
            mock
            pytest
            tox
            twine
            sphinx
            wheel
          ];
        in {
          default = pkgs.mkShell {
            packages = [
              self.packages.${system}.athenacli
              self.packages.${system}.redshiftcli
              pkgs.uv
            ] ++ devDeps;
            shellHook = ''
              export PYTHONPATH="$PWD"
            '';
          };
        });

      formatter = forAllSystems (system: (import nixpkgs { inherit system; }).nixpkgs-fmt);
    };
}
