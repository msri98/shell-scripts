#!/bin/sh
##################################################################################################
# This script reads MQ Messages on Hourly basis from HDFS location.
# 
##################################################################################################
CWD="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $CWD
echo "Current Working Dir: $CWD"
PARAM_FILE=`readlink -f ../../../../params/UTM/MEDEC/medec_mq_msg.prm`
echo  "Parameter file: $PARAM_FILE"
source $PARAM_FILE
echo "Current Partition: $MQ_MSG_PARTITION"

function next_partition()

 {

  #this function will return the next partition location to read MQ Messages from based on the current partition location.
  #this next partition location is written into the parameter file and  used in the next run. 
 

	arr=($(echo $1 | tr "/" "\n")) #Splits current partition by delimiter "/" to get into variables.
		local YEAR=${arr[3]}
		local MONTH=${arr[4]}
		local DAY=${arr[5]}
		local HOUR=${arr[6]}
		#echo $YEAR $MONTH $DAY $HOUR
		#"Calculate Hours for Next Partition"
		local I_HOUR=$(expr $HOUR + 0) #Converts string HOUR To Integer

		if [ $I_HOUR -lt  23 ]     #Checks if HOUR LT 23
		  then
                   
		   I_HOUR=$(($I_HOUR + 1)) #Incremnets HOUR By 1
		   if [ ${#I_HOUR} -lt 2 ] #Checks if HOUR is single Digit such as 1,2,3 ... then prepend 0 to 01,02,03 ...  
			  then
			 local  NEXT_HOUR="0$I_HOUR"
		   else
			 local NEXT_HOUR=$I_HOUR
		   fi
		   local NEXT_DAY=$DAY     #when incrementing HOUR, then no need to increment DAY and MONTH,YEAR
		   local NEXT_MONTH=$MONTH
		else
		   local NEXT_HOUR="00"  #Reset to 00 when the Current HOUR is 23

		   #"Calculate Days for Next Partition"
		   local NUM_DAYS=$(cal $MONTH $YEAR | xargs echo | awk '{print $NF}') #Get NUM DAYS in a Month

		   local I_DAY=$(expr $DAY + 0)  #Converts String DAY to Integer

		   if [ $I_DAY -lt  $NUM_DAYS ] #Check if DAY LT NUM_DAYS in a MONTH
			 then
                         I_DAY=$(($I_DAY + 1))   #Increments DAY By 1
			 if [ ${#I_DAY} -lt 2 ]  #Checks if DAY is Single Digit like 1,2,3...or 10,11,12...
				then
			       local NEXT_DAY="0$I_DAY"  #Prepend 0 to Single Digit DAY
			 else
			       local NEXT_DAY=$I_DAY  
			 fi
			       local NEXT_MONTH=$MONTH #When DAY is Incremented then no need to increment MONTH,YEAR
		   else
			  local NEXT_DAY="01"     #Reset to 01 when Current Partition Falls on LAST HOUR of the DAY 
			  #"Calculate Month for Next Partition"
			  local I_MONTH=$(expr $MONTH + 0)  #Converts String MONTH to Integer
			  if [ $I_MONTH -lt 12 ]        #Checks if MONTH LT 12
				then
				I_MONTH=$((I_MONTH + 1))  #Increments MONTH BY !
				if [ ${#I_MONTH} -lt 2 ]  #Checks for Single Digit month such as 1,2,3... then Prepend 0 as 01,02,03...
				   then
				  local  NEXT_MONTH="0$I_MONTH"  
				else
				  local NEXT_MONTH=$I_MONTH
				fi
			  else
			        local NEXT_MONTH="01"  #Reset MONTH to 01 when Current Partition Falls in DECEMBER and LAST HOUR of the 31 st DAY 
			  fi
		   fi
		fi
		#"Calculate Year for Next Partition"

		if [ "$NEXT_MONTH" == "01" ] && [ "$NEXT_DAY" == "01" ] && [ "$NEXT_HOUR" == "00" ]; #When MONTH,DAY and HOUR are reset to 01 then time to Increment YEAR BY 1 
			then
			local NEXT_YEAR=$(($(expr $YEAR + 0) + 1)) #Convert String YEAR to Integer and Increment it By 1
		else
			local NEXT_YEAR=$YEAR
		fi

		#echo "NEXT_PARTITION: $NEXT_HOUR $NEXT_DAY $NEXT_MONTH $NEXT_YEAR"
                local NEXT_PARTITION="$TR_SOURCE/$NEXT_YEAR/$NEXT_MONTH/$NEXT_DAY/$NEXT_HOUR"
               # sed -i -r "s#^(MQ_MSG_PARTITION=).*#\1$NEXT_PARTITION#" $PARAM_FILE  ##Update MQ_MSG_PARTITION parameter in the parameter file.
                echo $NEXT_PARTITION
 }

NXT_PATITION=$(next_partition $MQ_MSG_PARTITION)
echo "NEXT PARTITION: $NXT_PATITION"

if $(hadoop fs -test -e $MQ_MSG_PARTITION); then
      for dir in $(hadoop fs -ls -C $MQ_MSG_PARTITION) ; 
         do
           echo "${dir}"
         done
      echo "COMPLETED Reading all MQ Messages from "
else
    echo "HDFS Location $MQ_MSG_PARTITION Does not Exist to read MQ Messages from"
fi
