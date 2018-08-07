
clear

testName="TestReliableChurn"

for k in $( seq 1 5)
do
	str=""$k" th "$testName" start: "
	echo $str

    go test -run=$testName

    str=""$k" th "$testName" end: "
    echo $str
    echo ""
    echo ""
done

read name