Get-Command #list of commands
(get-command).count#1780 count of all commands
get-command '*service*'#return commands contain service name
(get-command '*service*').count#return count of service contain commands count

get-help Get-Service #return how to write syntaxs,aliase and name
get-help Get-Service -Examples #return how to use this command using examples
get-help Get-Service -Detailed #return in detalied about that command and parameters type
get-help Get-Service -full # return indetailed about syntax ,parameters,commanparameters
get-help Get-Service -Parameter 'inputobject'

get-service -name win* -ComputerName localhost #return status,name,displaname of names start with "win"
(get-command Get-Service).Parameters.Values #list of parameters by all columns
(get-command Get-Service).Parameters.Values |select name #list the parameters of that command by name column 

get-alias #list all commands which have alias
get-alias -Definition get-process #list of alias for particular  cmdlets
Get-Alias -Name ps
Get-psdrive #list the drives in locahost

cd Env: #cd-set-Location to enivronment
cd c: # change back to our main c drive
dir #list directory in c drive

get-content variable:* #to get predefined variables
$Error #predefined variable which contain error list
get-variable -Include profile,home
get-variable -Exclude profile,home

$cost=1000 #declaring int data variablr
$cost.GetType()
$cost|get-member
$cost=$cost.ToString() #convert int32 into string
$cost.GetType()  #get datatype
$cost|get-member #get methods of that dattype

get-alias -name ls #get the name of ls
ls EVN: # get-childitems in environment

#variable creation in powershell
$var=1000
$var
get-content variable:*
get-variable
get-variable -Name '*v*' #specific variable which start with 'var'

#cmdlets used on variables
get-command '*-variable' #list of variable cmdlets
get-help Get-Variable -full

#using those cmdlets to get variable
set-variable -name str -Value 'joshi'
get-variable -name str
#how to fing datatype
$str.GetType()

#using cmdlets to set varaiable
get-help set-variable -full
set-variable -name fruitsname -value 'mango','apple'
$fruitsname.gettype()
get-variable -name 'fruitsname'

# using cmdlet to clear varaiable data
Clear-Variable -name str #data in the variable is removed but variable is present
get-variable -name str

#using cmdlet to remove variable
Remove-Variable -name str
get-variable -name str #throws error as alreday variable is removed



#functions 
function get-myservice{
get-service -name "win*"
}
get-myservice


function greeting{
param(
[String]$name,
[int]$age)
write-host "Hello,$name!you are $age old"
}
greeting -name 'joshi' -age 30

#pipline
#byvalue
#bypropertyname
get-service


#get-command get-myservice




get-help New-AzStorageAccount -full
(get-command New-AzStorageAccount).parameters #to get parameters name in powershell for this cmdlets
 
get-variable #default variables in powershell

#know about EnableHierarchicalNamespace
get-help New-AzstorageAccount -parameter 'EnableHierarchicalNamespace'

#create variables
$resourcegroup="azuser1193pratice1"
$location="eastus" 

#blodstorage
new-AzStorageAccount -ResourceGroupName $resourcegroup -Name "psazuserdatalake" 
-Location $location -SkuName Standard_LRS  -Kind Storagev2

#datalake
new-AzStorageAccount -ResourceGroupName $resourcegroup -Name "psazuserdatalake" 
-Location $location -SkuName Standard_LRS  -Kind Storagev2 -EnableHierarchicalNamespace $true

#create azure sql database and sql sever
$resourcegroup="azuser1193pratice1"
$location="eastus"
$servername="psazuser1193sqlsever1"
$databasename="psazuser1193db1"
$administratorlogin="sqluser1"
$administratorpassword="azsql1@123"
$securepassword=ConvertTo-SecureString -String $administratorpassword -AsPlainText -Force

#how to create azsqlserver
get-help New-AzSqlServer -detailed
(get-command New-AzSqlServer).Parameters
get-help New-Object -full
(get-command New-Object).Parameters
New-AzSqlServer -ResourceGroupName $resourcegroup 
                -ServerName $servername 
                -Location $location 
                -SqlAdministratorCredentials (New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $administratorlogin,$securepassword)

#how to create azsqldatabase
get-help New-AzSqlDatabase -detailed
(get-command New-AzSqlDatabase).Parameters
New-AzSqlDatabase -ResourceGroupName $resourcegroup 
                  -ServerName $servername
                  -DatabaseName $databasename 
                  -Edition "standard"

#how to get username and password using for already created database 

  

 

