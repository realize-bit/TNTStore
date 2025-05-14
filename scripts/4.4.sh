#!/bin/bash

disk="/dev/nvme1n1"
dir="/home/jongseok/tnt/evaluation/2025_paper_results/4/4.4"

function umount_mount {
	sudo umount /scratch0
	sudo mkfs.ext4 -F ${disk}
	sudo mount ${disk} /scratch0/
	sudo rm -rf /scratch0/*
	sudo chown jongseok:jongseok /scratch0/
	sudo mkdir /scratch0/kvell
	sleep 5
}

#workload=( "C_Zipf" "C_Unif" "A_Zipf" "A_Unif" "DBBENCH" )
#mem=( "8" )
#
#for ((i=1; i<=3; i++))
#do
#	for m in ${mem[@]}
#	do
#		for w in ${workload[@]}
#		do
#			echo ${w} - ${m} - ${i}
#			umount_mount
#			numactl -N 0 -m 0 ./bin/4.4/${m}G_100M_${w}_DESCEND 1 48 12 > ${dir}/DESCEND_100M_${m}G_${w}_60t_${i}
#			sleep 5
#		done
#	done
#done
#
#for ((i=1; i<=3; i++))
#do
#	for m in ${mem[@]}
#	do
#		for w in ${workload[@]}
#		do
#			echo ${w} - ${m} - ${i}
#			umount_mount
#			numactl -N 0 -m 0 ./bin/4.4/${m}G_100M_${w}_ASCEND 1 48 12 > ${dir}/ASCEND_100M_${m}G_${w}_60t_${i}
#			sleep 5
#		done
#	done
#done

workload=( "A_Zipf" )
mem=( "8" )

#for ((i=1; i<=1; i++))
#do
#	for m in ${mem[@]}
#	do
#		for w in ${workload[@]}
#		do
#			echo ${w} - ${m} - ${i}
#			umount_mount
#			numactl -N 0 -m 0 ./bin/4.4/${m}G_100M_${w}_DESCEND_OFF_RB_RI 1 48 12 > ${dir}/DESCEND_OFF_RB_RI_100M_${m}G_${w}_60t_${i}
#			sleep 5
#		done
#	done
#done

for ((i=1; i<=1; i++))
do
	for m in ${mem[@]}
	do
		for w in ${workload[@]}
		do
			echo ${w} - ${m} - ${i}
			umount_mount
			numactl -N 0 -m 0 ./bin/4.4/${m}G_100M_${w}_DESCEND_OFF_RB_RI05 1 48 12 > ${dir}/DESCEND_OFF_RB_RI05_100M_${m}G_${w}_60t_${i}
			sleep 5
		done
	done
done

for ((i=1; i<=1; i++))
do
	for m in ${mem[@]}
	do
		for w in ${workload[@]}
		do
			echo ${w} - ${m} - ${i}
			umount_mount
			numactl -N 0 -m 0 ./bin/4.4/${m}G_100M_${w}_DESCEND_OFF_RB_RI10 1 48 12 > ${dir}/DESCEND_OFF_RB_RI10_100M_${m}G_${w}_60t_${i}
			sleep 5
		done
	done
done

for ((i=1; i<=1; i++))
do
	for m in ${mem[@]}
	do
		for w in ${workload[@]}
		do
			echo ${w} - ${m} - ${i}
			umount_mount
			numactl -N 0 -m 0 ./bin/4.4/${m}G_100M_${w}_DESCEND_OFF_RB_RI20 1 48 12 > ${dir}/DESCEND_OFF_RB_RI20_100M_${m}G_${w}_60t_${i}
			sleep 5
		done
	done
done

for ((i=1; i<=1; i++))
do
	for m in ${mem[@]}
	do
		for w in ${workload[@]}
		do
			echo ${w} - ${m} - ${i}
			umount_mount
			numactl -N 0 -m 0 ./bin/4.4/${m}G_100M_${w}_DESCEND_OFF_RB_RI40 1 48 12 > ${dir}/DESCEND_OFF_RB_RI40_100M_${m}G_${w}_60t_${i}
			sleep 5
		done
	done
done

