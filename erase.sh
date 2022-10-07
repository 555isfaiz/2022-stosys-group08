
#!/bin/bash
eval "sudo rm -rf /tmp/testdb"
eval "sudo rm -rf /tmp/shadowdb"
eval "sudo ./nvme-cli/nvme zns reset-zone -a /dev/nvme0n1"
echo "reset complete"
