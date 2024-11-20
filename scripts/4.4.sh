#!/bin/bash

disk="/dev/nvme1n1"
RDIR="/home/jongseok/tnt/evaluation/paper_results/4/4.4/tntstore"

function umount_mount {
	sudo swapoff -a
	sudo umount /scratch0

	sudo mkfs.ext4 -E nodiscard -F ${disk}
	sudo mount ${disk} /scratch0
	sudo rm -rf /scratch0/*
	sudo chown jongseok:jongseok /scratch0
	sudo mkdir /scratch0/kvell

	dd if=/dev/zero of=/scratch0/swap.img bs=1024 count=8388608
	sudo chmod 600 /scratch0/swap.img 
	sudo chown root:root /scratch0/swap.img 
	mkswap /scratch0/swap.img 
	sudo swapon /scratch0/swap.img

	sudo echo 3 > /proc/sys/vm/drop_caches
	sleep 5
}



ulimit -n 500000
echo 10737418240 > /sys/fs/cgroup/memory/eval/memory.limit_in_bytes
workload=( "C_Zipf" "C_Unif" "A_Zipf" "A_Unif" )
mem=( "3" "4" "5" "6" "7" "8" )

for ((i=1; i<=3; i++))
do
	for m in ${mem[@]}
	do
		for w in ${workload[@]}
		do
			echo 10737418240 > /sys/fs/cgroup/memory/eval/memory.limit_in_bytes
			echo ${m}G - $w - ${i}
			umount_mount
			echo $$ > /sys/fs/cgroup/memory/eval/cgroup.procs
			cat /proc/vmstat | grep psw > ${RDIR}/swap_100M_${m}G_${w}_60t_${i}
			numactl -N 0 -m 0 ./bin/twice/${m}G_100M_${w} 1 48 12 >> ${RDIR}/swap_100M_${m}G_${w}_60t_${i}
			cat /proc/vmstat | grep psw >> ${RDIR}/swap_100M_${m}G_${w}_60t_${i}
			sleep 5
		done
	done
done
