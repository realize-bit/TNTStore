#!/bin/bash

disk="/dev/nvme1n1"
dir="/home/jongseok/tnt/evaluation/2025_paper_results/4/4.6"

function umount_mount {
	sudo umount /scratch0
	sudo mkfs.ext4 -F ${disk}
	sudo mount ${disk} /scratch0/
	sudo rm -rf /scratch0/*
	sudo chown jongseok:jongseok /scratch0/
	sudo mkdir /scratch0/kvell
	sleep 5
}

ulimit -n 500000
workload=( "A_Zipf" "A_Unif" "E_Zipf" "E_Unif" "C_Zipf" "C_Unif" "DBBENCH" )
kvsize=( "2048" "1024" "512" "256" "128" "64" )

for ((i=1; i<=3; i++))
do
	for s in ${kvsize[@]}
	do
		for w in ${workload[@]}
		do
			echo ${w} - ${s}B kvsize - ${i}
			umount_mount
			numactl -N 0 -m 0 ./bin/4.6/${s}_4G_100M_${w} 1 48 12 > ${dir}/${s}_100M_4G_${w}_60t_${i}
			sleep 5
		done
	done
done
