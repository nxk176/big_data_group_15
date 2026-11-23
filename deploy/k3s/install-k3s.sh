#!/usr/bin/env bash
set -euo pipefail

# Installs a single-node k3s server (Kubernetes) on this Linux host: Ubuntu, or WSL2 with systemd enabled.
# Needs sudo. The kubeconfig is made readable so kubectl works without sudo.
version="${K3S_VERSION:-v1.37.2+k3s1}"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "k3s runs on Linux: use WSL2 on Windows or a Linux VM (e.g. Lima) on macOS" >&2
  exit 1
fi
if [[ "$(ps -p 1 -o comm=)" != "systemd" ]]; then
  echo "systemd is not PID 1. On WSL2 add '[boot]\\nsystemd=true' to /etc/wsl.conf, run 'wsl --shutdown', reopen" >&2
  exit 1
fi
if command -v k3s >/dev/null 2>&1 && systemctl is-active --quiet k3s; then
  printf 'k3s already running: %s\n' "$(k3s --version | head -1)"
else
  curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION="$version" \
    INSTALL_K3S_EXEC="server --write-kubeconfig-mode 644" sh -
fi

mkdir -p "$HOME/.kube"
cp /etc/rancher/k3s/k3s.yaml "$HOME/.kube/config"
chmod 600 "$HOME/.kube/config"
for _ in {1..60}; do
  if kubectl get nodes 2>/dev/null | grep -q " Ready "; then
    kubectl get nodes -o wide
    exit 0
  fi
  sleep 2
done
echo "k3s node did not become Ready within 120 seconds; see 'sudo journalctl -u k3s'" >&2
exit 1
