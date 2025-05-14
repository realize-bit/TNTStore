#!/bin/bash

disk="/dev/nvme1n1"
dir="/home/jongseok/tnt/evaluation/paper_results/4/4.3/tntstore/key_dist"

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
workload=( "E_Unif" "DBBENCH" )
keydist=( "osm" "amazon" )

for ((i=1; i<=3; i++))
do
	for k in ${keydist[@]}
	do
		for w in ${workload[@]}
		do
			echo ${w} - ${k} - ${i}
			umount_mount
			numactl -N 0 -m 0 ../bin/key_dist/${k}_8G_100M_${w} 1 48 12 > ${dir}/${k}_100M_8G_${w}_60t_${i}
			sleep 5
		done
	done
done
