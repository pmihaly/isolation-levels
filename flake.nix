{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
    pre-commit-hooks.url = "github:cachix/pre-commit-hooks.nix";
    treefmt-nix.url = "github:numtide/treefmt-nix";
  };

  outputs = { self, nixpkgs, flake-utils, pre-commit-hooks, treefmt-nix, }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        deps = with pkgs; [ go ];
        devDeps = with pkgs; [ delve ];

        treefmtEval = treefmt-nix.lib.evalModule pkgs ({ pkgs, ... }: {
          projectRootFile = "flake.nix";
          programs.nixfmt.enable = true;
          programs.gofmt.enable = true;
        });
      in {
        packages.default = pkgs.buildGoModule {
          name = "isolation-levels";
          src = ./.;
          vendorHash = null;
          doCheck = true;
        };

        checks = {
          pre-commit-check = pre-commit-hooks.lib.${system}.run {
            src = ./.;
            hooks = {
              nixfmt.enable = true;
              gofmt.enable = true;
            };
          };
          formatting = treefmtEval.config.build.check self;
        };

        devShell = nixpkgs.legacyPackages.${system}.mkShell {
          inherit (self.checks.${system}.pre-commit-check) shellHook;
          buildInputs = deps ++ devDeps;
        };

        formatter = treefmtEval.config.build.wrapper;
      });
}
