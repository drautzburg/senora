# $Id: copy_user.sh,v 1.1 2011/03/25 14:36:26 ekicmust Exp $

copy_user.pl > copy_user.sql

if [ $? -gt 0 ]
then
  exit 1
fi

sqlplus -s /  << EOF | tee copy_user.log
  @ copy_user.sql
EOF

# $Log: copy_user.sh,v $
# Revision 1.1  2011/03/25 14:36:26  ekicmust
# #147852 initial version. Instead of senora.exe the perl version will be taken now.
#
# Revision 1.1.1.1  2006/05/05 20:13:35  martin
#
# initial import
#
# Revision 1.1  2004/02/12 10:57:53  DrauMart
# Initial Checkin
#
# Revision 1.2  2000/11/11 07:48:59  rvsutherland
# Added CVS tags
#

