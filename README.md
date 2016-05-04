# nbd server
start with:
java -cp /path/to/classes de.m3y3r.dasdnbd.Server /path/to/compressed-ckd-image

# nbd client
sudo modprobe nbd
sudo nbd-client -N LINUX.V0X0120.PART0003.NATIVE localhost /dev/nbd0
sudo mount /dev/nbd0 /mnt -o ro

