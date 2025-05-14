#!/bin/bash

disk="/dev/nvme1n1"
dir="/home/jongseok/tnt/evaluation/2025_paper_results/4/4.2/tntstore"

function umount_mount {
	sudo umount /scratch0
	sudo mkfs.ext4 -F ${disk}
	sudo mount ${disk} /scratch0/
	sudo rm -rf /scratch0/*
	sudo chown jongseok:jongseok /scratch0/
	sudo mkdir /scratch0/kvell
	sleep 5
}

workload=( "C_Zipf" "C_Unif" "B_Zipf" "B_Unif" "E_Zipf" "E_Unif" "A_Zipf" "A_Unif" )
mem=( "32" "16" "8" "4" "2" )

for ((i=1; i<=3; i++))
do
	for m in ${mem[@]}
	do
		for w in ${workload[@]}
		do
			echo ${w} - ${m} - ${i}
			umount_mount
			numactl -N 0 -m 0 ./bin/4.2/${m}G_100M_${w} 1 48 12 > ${dir}/100M_${m}G_${w}_60t_${i}
			sleep 5
		done
	done
done
