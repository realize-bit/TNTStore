#!/bin/bash

disk="/dev/nvme1n1"
dir="/home/jongseok/tnt/evaluation/paper_results/4/4.2/kvell/test"

function umount_mount {
	sudo umount /scratch0
	sudo mkfs.ext4 -E nodiscard -F ${disk}
	sudo mount ${disk} /scratch0/
	sudo rm -rf /scratch0/*
	sudo chown jongseok:jongseok /scratch0/
	sudo mkdir /scratch0/kvell
	sleep 5
}

workload=( "A_Zipf" "A_Unif" )
#workload=( "E_Zipf_12L" "E_Unif_12L" "DBBENCH_12L" )
mem=( "32" "16" "8" "4" "2" )

for ((i=1; i<=5; i++))
do
	for m in ${mem[@]}
	do
		for w in ${workload[@]}
		do
			echo ${w} - ${m} - ${i}
			umount_mount
			numactl -N 0 -m 0 ./bin/${m}G_100M_${w} 1 60 > ${dir}/100M_${m}G_${w}_60t_${i}
			sleep 5
		done
	done
done

#workload=( "DBBENCH_OURS" "DBBENCH" )
#
#for ((i=1; i<=5; i++))
#do
#	for m in ${mem[@]}
#	do
#		for w in ${workload[@]}
#		do
#			echo ${w} - ${m} - ${i}
#			umount_mount
#			numactl -N 0 -m 0 ./bin/${m}G_100M_${w} 1 60 > ${dir}/100M_${m}G_${w}_60t_${i}
#			sleep 5
#		done
#	done
#done
#
#

