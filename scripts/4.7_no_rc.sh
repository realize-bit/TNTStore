#!/bin/bash

disk="/dev/nvme1n1"
dir="/home/jongseok/tnt/evaluation/paper_results/4/4.7"

function umount_mount {
	sudo umount /scratch0
	sudo mkfs.ext4 -E nodiscard -F ${disk}
	sudo mount ${disk} /scratch0/
	sudo rm -rf /scratch0/*
	sudo chown jongseok:jongseok /scratch0/
	sudo mkdir /scratch0/kvell
	sleep 5
}

ulimit -n 500000
#workload=( "A_Zipf" "A_Unif" "E_Zipf" "E_Unif" "C_Zipf" "C_Unif" "DBBENCH" )
workload=( "A_Zipf" "A_Unif" "E_Zipf" "E_Unif" "C_Zipf" "C_Unif" )
#workload=( "DBBENCH" )
#subtree=( "8" "16" "32" "64" "128" "256" "512" )

for ((i=1; i<=5; i++))
do
	for w in ${workload[@]}
	do
		echo no_rc ${w} 4G - ${i}
		umount_mount
		numactl -N 0 -m 0 ../bin/no_rc/4G_100M_${w} 1 48 12 > ${dir}/no_rc_100M_4G_${w}_60t_${i}
		df >> ${dir}/no_rc_100M_4G_${w}_60t_${i}
		sleep 5
	done
done
