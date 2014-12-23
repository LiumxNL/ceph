#!/bin/bash
echo "--------------------------------------"
echo "|****rbd merge-diff function test****|"
echo "--------------------------------------"

img1=generate
img2=checkout
outfile=mtp/$img1
dir=diffdir
if [ ! -d "$dir" ]; then
  mkdir $dir
fi
if [ ! -d "mtp" ];then
  mkdir mtp
fi

check(){
  if [ -e $1 -a "$1" != "/" ]; then
  rm -f $1*;
  fi
}
diff(){
  if [ $# -eq 1 ];then
  cd $dir
  rbd snap create "$img1@$1"
  check $1
  rbd export-diff "$img1@$1" "$1"
  cd - >>/dev/null
  echo "$1"
  else
    if [ $# -eq 2 ];then
    cd $dir
    rbd snap create "$img1@$2"
    check "$1"t"$2"
    rbd export-diff "$img1@$2" --from-snap $1 "$1"t"$2"
    cd - >>/dev/null
    echo "$1"t"$2"
    else exit -1
    fi
  fi
}
merge(){
  if [ $# -ne 2 ];then
  exit -1
  else
  cd $dir
  check "$1&$2"
  rbd merge-diff -first "$1" -second "$2" "$1&$2"
  cd - >>/dev/null
  echo "$1&$2"
  fi
}
compare(){
  md5_1=`md5sum mtp/$img1 | awk '{print $1}'`
  md5_2=`md5sum mtp/$img2 | awk '{print $1}'`
  echo $md5_1
  echo $md5_2
  if [ "$md5_1" = "$md5_2" ]; then
  echo pass
  echo pass >>result.log
  else
  echo fail
  echo fail >>result.log
fi
}

rebuild(){	##clear enviroment
fusermount -u mtp 2>/dev/null
##create rbd_img.
imgs=(`rbd ls`)
count=1
echo "remove all img"
for img in ${imgs[*]}
  do
  if [ "$img" = "$img1" -o "$img" = "$img2" ];then
    echo imgname:$img
    rbd snap purge $img
    rbd rm $img
    while [ $? -ne 0 ]; do
      echo -n "please wait:"	##wait for watcher wholely calm down.
      for i in 5 4 3 2 1;do
        echo -n "$i"s
        sleep 1
        echo -ne "\b\b"
      done
      echo -ne "\r"
      let count=count+1
      if [ "$count" -gt 10 ];then
        exit -1
      fi
      rbd rm $img 2>/dev/null
    done
  fi
  done
echo "rbd create $img1"
rbd create $img1 --size 10
echo "rbd create $img2"
rbd create $img2 --size 10

##rbd-fuse
echo "rbd-fuse mtp"
rbd-fuse mtp
if [ $? -ne 0 ];then
exit -1
fi
}

##construct test sample
echo -n "test sample select(zero for all,minus exit):"
read choice
if [ $choice -lt 0 ];then
exit -1
fi

##sample1
if [ $choice -eq 1 -o $choice -eq 0 ];then
  echo "[**  sample1  **]"
  rebuild
  echo "operating..."
  dd if=/dev/urandom of=$outfile bs=2M count=1 seek=2 conv=notrunc
  d1=`diff s101`
  echo "diff:$d1"
  dd if=/dev/urandom of=$outfile bs=3M count=1 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=2M count=1 seek=4 conv=notrunc
  d2=`diff s101 s102`
  echo "diff:$d2"
  d1md2=`merge $d1 $d2`
  echo "diff:$d1md2"
  cd $dir; rbd import-diff $d1md2 $img2;cd - >>/dev/null
  echo -n `date --rfc-3339='seconds'` >>result.log
  echo -ne "\tsample1\t" >>result.log
  compare
  if [ $choice -gt 0 ];then 
    exit 0
  fi
fi

##sample2
if [ $choice -eq 2 -o $choice -eq 0 ];then
  echo "[**  sample2  **]"
  rebuild
  echo "operating..."
  dd if=/dev/urandom of=$outfile bs=3M count=1 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=2560K count=1 seek=2 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=2M count=1 seek=4 conv=notrunc
  d1=`diff s201`
  echo "diff:$d1"
  dd if=/dev/urandom of=$outfile bs=2M count=1 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=3M count=1 seek=2 conv=notrunc
  d2=`diff s201 s202`
  echo "diff:$d2"
  d1md2=`merge $d1 $d2`
  echo "diff:$d1md2"
  cd $dir; rbd import-diff $d1md2 $img2;cd - >>/dev/null
  echo -n `date --rfc-3339='seconds'` >>result.log
  echo -ne "\tsample2\t" >>result.log
  compare
  if [ $choice -gt 0 ];then
    exit 0
  fi
fi

##sample3
if [ $choice -eq 3 -o $choice -eq 0 ];then
  echo "[**  sample3  **]"
  rebuild
  echo "operating..."
  dd if=/dev/urandom of=$outfile bs=2M count=1 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=3M count=1 seek=2 conv=notrunc
  d1=`diff s301`
  echo "diff:$d1"
  dd if=/dev/urandom of=$outfile bs=3M count=1 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=2560K count=1 seek=2 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=2M count=1 seek=4 conv=notrunc
  d2=`diff s301 s302`
  echo "diff:$d2"
  d1md2=`merge $d1 $d2`
  echo "diff:$d1md2"
  cd $dir; rbd import-diff $d1md2 $img2;cd - >>/dev/null
  echo -n `date --rfc-3339='seconds'` >>result.log
  echo -ne "\tsample3\t" >>result.log
  compare
  if [ $choice -gt 0 ];then
    exit 0
  fi
fi

##sample4
if [ $choice -eq 4 -o $choice -eq 0 ];then
  echo "[**  sample4  **]"
  rebuild
  echo "operating..."
  dd if=/dev/urandom of=$outfile bs=2M count=3 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=1M count=1 seek=7 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=1M count=1 seek=9 conv=notrunc
  d1=`diff s401`
  echo "diff:$d1"
  dd if=/dev/urandom of=$outfile bs=512K count=2 seek=1 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=512K count=6 seek=5 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=378K count=4 seek=18 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=512K count=2 seek=18 conv=notrunc
  d2=`diff s401 s402`
  echo "diff:$d2"
  d1md2=`merge $d1 $d2`
  echo "diff:$d1md2"
  cd $dir; rbd import-diff $d1md2 $img2;cd - >>/dev/null
  echo -n `date --rfc-3339='seconds'` >>result.log
  echo -ne "\tsample4\t" >>result.log
  compare
  if [ $choice -gt 0 ];then
    exit 0
  fi
fi

##sample5
if [ $choice -eq 5 -o $choice -eq 0 ];then
  echo "[**  sample5  **]"
  rebuild
  echo "operating..."
  dd if=/dev/urandom of=$outfile bs=512K count=2 seek=1 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=512K count=6 seek=5 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=378K count=4 seek=18 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=512K count=2 seek=18 conv=notrunc
  d1=`diff s501`
  echo "diff:$d1"
  dd if=/dev/urandom of=$outfile bs=2M count=3 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=1M count=1 seek=7 conv=notrunc
  dd if=/dev/urandom of=$outfile bs=1M count=1 seek=9 conv=notrunc
  d2=`diff s501 s502`
  echo "diff:$d2"
  d1md2=`merge $d1 $d2`
  echo "diff:$d1md2"
  cd $dir; rbd import-diff $d1md2 $img2;cd - >>/dev/null
  echo -n `date --rfc-3339='seconds'` >>result.log
  echo -ne "\tsample5\t" >>result.log
  compare
  if [ $choice -gt 0 ];then
    exit 0
  fi
fi

##sample6
if [ $choice -eq 6 -o $choice -eq 0 ];then
  echo "[**  sample6  **]"
  rebuild
  echo "operating..."
  dd if=/dev/urandom of=$outfile bs=4M count=2 conv=notrunc
  rbd resize --size 5 $img1 --allow-shrink
  d1=`diff r1`
  echo "diff:$d1"
  rbd resize --size 20 $img1
  dd if=/dev/urandom of=$outfile bs=4M count=2 seek=3 conv=notrunc
  d2=`diff r1 r2`
  echo "diff:$d2"
  rbd resize --size 8 $img1 --allow-shrink
  dd if=/dev/urandom of=$outfile bs=4M count=1 seek=1 conv=notrunc
  d3=`diff r2 r3`
  echo "diff:$d3"
  rbd resize --size 10 $img1
  d4=`diff r3 r4`
  echo "diff:$d4"
  d1md2=`merge $d1 $d2`
  echo "diff:$d1md2"
  d3md4=`merge $d3 $d4`
  echo "diff:$d3md4"
  d1td4=`merge $d1md2 $d3md4`
  echo "diff:$d1td4"
  cd $dir; rbd import-diff $d1td4 $img2;cd - >>/dev/null
  echo -n `date --rfc-3339='seconds'` >>result.log
  echo -ne "\tsample6\t" >>result.log
  compare
  if [ $choice -gt 0 ];then
    exit 0
  fi
fi

##sample7
if [ $choice -eq 7 -o $choice -eq 0 ];then
  echo "[**  sample7  **]"
  rebuild
  echo "operating..."
  dd if=/dev/urandom of=$outfile bs=4M count=2 conv=notrunc
  rbd resize --size 5 $img1 --allow-shrink
  d1=`diff sr1`
  echo "diff:$d1"
  rbd resize --size 20 $img1
  dd if=/dev/urandom of=$outfile bs=4M count=2 seek=3 conv=notrunc
  d2=`diff sr1 sr2`
  echo "diff:$d2"
  rbd resize --size 8 $img1 --allow-shrink
  dd if=/dev/urandom of=$outfile bs=4M count=1 seek=1 conv=notrunc
  d3=`diff sr2 sr3`
  echo "diff:$d3"
  rbd resize --size 10 $img1
  d4=`diff sr3 sr4`
  echo "diff:$d4"
  d1md2=`merge $d1 $d2`
  echo "diff:$d1md2"
  d1td3=`merge $d1md2 $d3`
  echo "diff:$d1td3"
  d1td4=`merge $d1td3 $d4`
  echo "diff:$d1td4"
  cd $dir; rbd import-diff $d1td4 $img2;cd - >>/dev/null
  echo -n `date --rfc-3339='seconds'` >>result.log
  echo -ne "\tsample7\t" >>result.log
  compare
  if [ $choice -gt 0 ];then
    exit 0
  fi
fi


