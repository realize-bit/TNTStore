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
workload=( "A_Zipf" "A_Unif" "E_Zipf" "E_Unif" "C_Zipf" "C_Unif" "DBBENCH" )
ioworker=( "12" "20" "30" "40" "48" )

for ((i=1; i<=3; i++))
do
	for io in ${ioworker[@]}
	do
		for w in ${workload[@]}
		do
			dt=$((60-${io}))
			echo ${w} - ${io}/${dt} - ${i}
			umount_mount
			numactl -N 0 -m 0 ../bin/fix_add_in_tree_update/8G_100M_${w} 1 ${io} ${dt} > ${dir}/100M_8G_${w}_${io}w${dt}d_${i}
			sleep 5
		done
	done
done
