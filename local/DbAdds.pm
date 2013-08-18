# --------------------------------------------------------------------------------  
# NAME
#	DbAdds - database additional commands
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# 	sql->run("GRANT create SYNONYM to $newdbuser");
# $Id: DbAdds.pm,v 1.2 2011/01/12 09:57:10 ekicmust Exp $
# 	sql->run("GRANT create database link to $newdbuser");
# $Id: DbAdds.pm,v 1.2 2011/01/12 09:57:10 ekicmust Exp $
# 	create ADMSPM_APP for new ADMSPM added;
# $Id: DbAdds.pm,v 1.2 2011/01/12 09:57:10 ekicmust Exp $
# 	sql->run("GRANT query rewrite to $newdbuser");
# $Id: DbAdds.pm,v 1.2 2011/01/12 09:57:10 ekicmust Exp $
# 	qs_gateway variant added;
# $Id: DbAdds.pm,v 1.2 2011/01/12 09:57:10 ekicmust Exp $
# 	GRANT IMP_FULL_DATABASE  to admspm added;
# $Id: DbAdds.pm,v 1.2 2011/01/12 09:57:10 ekicmust Exp $
# 	GRANT analyze any to admspm added;
# $Id: DbAdds.pm,v 1.2 2011/01/12 09:57:10 ekicmust Exp $
# 	Get owned grants of a user added;
#
# --------------------------------------------------------------------------------  
# $log$
# --------------------------------------------------------------------------------  

package DbAdds;
use strict;
use Plugin;
use SessionMgr;
use Text::Wrap;
use Getopt::Long;
use File::Basename;
use Misc;

Getopt::Long::Configure("no_ignore_case");
Getopt::Long::Configure("bundling");


use vars qw 
(
 @ISA
 $COMMANDS
);

@ISA = qw (
	   Plugin
);
# ------------------------------------------------------------
sub new {
	my ($That) = @_;
	my $class = ref($That) || $That;
	my $this;

	$this->{commands} = $COMMANDS;

	bless ($this, $class);

	return $this;
}



# ------------------------------------------------------------
$COMMANDS->{shalluser} =
  {
   alias => "shau",
   description => "show all user",
   synopsis => 
   [
    [],
    ["-l",undef,"show long format"],
    ["-o", "columns", 'order by these columns'],
   ],
   routine => \&shalluser
  };

sub shalluser {
	my ($this, $argv, $opts) = @_;
	my $orderBy= $opts->{o} ? $opts->{o} :"1,2";
	my $scolumns="created, username";
	$scolumns="*" if $opts->{l};
#	$orderBy="9,1" if $opts->{l};

	sql->run("
		select $scolumns
		from dba_users order by $orderBy desc
	");

}
# ------------------------------------------------------------
$COMMANDS->{shtablespacefileuse} =
  {
   alias => "shtsfu",
   description => "show table space file use of all users",
   synopsis => 
   [
    [],
    ["-t",undef,"group by table space"],
    ["-o", "columns", 'order by these columns'],

   ],
   routine => \&shtablespacefileuse
  };

sub shtablespacefileuse {
	my ($this, $argv, $opts) = @_;
	my $orderBy= $opts->{o} ? $opts->{o} :"2,3";
	my $columns="owner, tablespace_name";
	$columns="tablespace_name" if $opts->{t};
	sql->run(qq(
		select owner, tablespace_name, sum(bytes)/1048576 "SIZE/MByte" 
		from dba_extents group by owner, tablespace_name order by $orderBy desc
	));
}

# ------------------------------------------------------------
$COMMANDS->{shtablespacefiles} =
  {
   alias => "shtsf",
   description => "show table space files",
   synopsis => 
   [
    [],
    ["-l",undef,"show long format"],
   ],
   routine => \&shtablespacefiles
  };

sub shtablespacefiles {
	my ($this, $argv, $opts) = @_;
  my $scolumns="file_name, tablespace_name, bytes, maxbytes";
  $scolumns="*" if $opts->{l};

	sql->run(qq(
		select $scolumns from dba_data_files order by tablespace_Name, File_Name
	));

}

# ------------------------------------------------------------
$COMMANDS->{"shinfo"} =
  {
   alias => "shinf",
   description => "show general info of the instance",
   synopsis => 
   [
    [""],
   ],
   routine => \&shinfo
  };

sub shinfo {
	my ($this, $argv, $opts) = @_;

	sql->run(q( 
		select instance_name INSTANCE, SUBSTR (host_name, 1, 20) host, version, 
			startup_time STARTUP, status, database_status db_status, archiver, logins 
		from v$instance
	));

}

# ------------------------------------------------------------
$COMMANDS->{shgrants} =
  {
   alias => "shgrant",
   description => "show general owned grants of the given user",
   synopsis => 
   [
    [],
    ["-u","dbuser","User Name which grants should be listed)"],
   ],
   routine => \&shgrants
  };

sub shgrants {
	my ($this, $argv, $opts) = @_;
	my $dbuser="admspm";
	$dbuser=$opts->{u} if $opts->{u};

	sql->run(qq( 
		select
		    lpad(' ', 2*level) || granted_role "User, his roles and privileges"
		from
		(
			/* THE USERS */
			select 
				null     grantee, 
				username granted_role
			from 
				dba_users
			where
				username like upper('$dbuser')
				/* THE ROLES TO ROLES RELATIONS */ 
			union
			select 
				grantee,
				granted_role
			from
				dba_role_privs
			/* THE ROLES TO PRIVILEGE RELATIONS */ 
			union
			select
				grantee,
				privilege
			from
				dba_sys_privs
		)
		start with grantee is null
		connect by grantee = prior granted_role
	));

}

# ------------------------------------------------------------
$COMMANDS->{create_user} =
  {
   alias => "cru",
   description => "create database user",
   synopsis => 
   [
    [],
    ["-u","newdbuser","User Name to be modified (DEL|Create|SetGrants)"],
    ["-d",undef,"Drop the user first"],
    ["-g",undef,"Don't Delete OR/AND Create User. Set only Grants"],
    ["-n",undef,"Create Data files for new ADMSPM (AS_[DATA|INDEX])"],
    ["-a",undef,"Create Data files for new ADMSPM_APP (AS_[DATA|INDEX])(No Resource)"],
    ["-q",undef,"Create Data files for new QS_GATEWAY (QS_GATEWAY)"],
    ["-r",undef,"NO revoke option (only for spm and mrm user)"],
    ["-s",undef,"Environment is the seq-instance of old ADMSPM"],
   ],
   routine => \&create_user
  };

sub create_user {
	my ($this, $argv, $opts) = @_;
	my $newdbuser="mekici";
	$newdbuser=$opts->{u} if $opts->{u};

	my $resourcetyp="yes";
  $resourcetyp="no" if $opts->{a};

	my $dbtyp="cc";
  $dbtyp ="as" if $opts->{a};
  $dbtyp ="as" if $opts->{n};
  $dbtyp ="qs" if $opts->{q};

	my $revoketyp="yes";
  $revoketyp="no" if $opts->{r};

	my $seqtyp="no";
  $seqtyp="yes" if $opts->{s};

	my $qstyp="no";
  $qstyp="yes" if $opts->{q};

	my $onlygrant="no";
  $onlygrant="yes" if $opts->{g};

  my $data="$dbtyp"."_data";
  my $index="$dbtyp"."_index";
	my $asi_data="asi_data";
	my $asi_index="asi_index";

  if ($qstyp eq "yes") {
      $data="qs_gateway";
      $index="$data";
  }
  if ($onlygrant eq "no") {

     if ($opts->{d}) {
        sql->run("DROP USER $newdbuser CASCADE");
     }

#  sql->run("whenever sqlerror exit success");
 	sql->run(qq( 
    CREATE user $newdbuser identified by $newdbuser default tablespace $data temporary tablespace temp
	));

  }

  if ($resourcetyp eq "yes") {
    	sql->run("GRANT resource to $newdbuser");
  }

  if ($revoketyp eq "yes") {
     sql->run("REVOKE unlimited tablespace from $newdbuser");
  }

	sql->run(qq(ALTER user $newdbuser quota unlimited on $data));
  if ($seqtyp eq "no" or $qstyp eq "no") {
   	 sql->run(qq(ALTER user $newdbuser quota unlimited on $index));
  }
  
  if ($dbtyp eq "as") {
     sql->run(qq(ALTER user $newdbuser quota unlimited on $asi_data));
   	 sql->run(qq(ALTER user $newdbuser quota unlimited on $asi_index));
  }

	sql->run("GRANT create any view to $newdbuser");
	sql->run("GRANT create any table to $newdbuser");
	sql->run("GRANT create PUBLIC SYNONYM to $newdbuser");
	sql->run("GRANT create SYNONYM to $newdbuser");
	sql->run("GRANT create sequence to $newdbuser");
	sql->run("GRANT create database link to $newdbuser");

	sql->run("GRANT analyze any to $newdbuser");
	sql->run("GRANT query rewrite to $newdbuser");

	sql->run("GRANT execute on sys.dbms_alert to $newdbuser");
	sql->run("GRANT execute on sys.dbms_pipe to $newdbuser");
  if ($seqtyp eq "no") {
     sql->run("GRANT execute on sys.dbms_lock to $newdbuser");
     sql->run("GRANT execute on dbms_obfuscation_toolkit to $newdbuser");
#-- - 	sql->run("GRANT execute on dbms_obfuscation_toolkit to public");
  }
	sql->run("GRANT select any dictionary to $newdbuser");
	sql->run("GRANT select on v_\$session to $newdbuser");
	sql->run("GRANT select_catalog_role to $newdbuser");
  if ($seqtyp eq "no" or $qstyp eq "no") {
    	sql->run("GRANT javasyspriv to $newdbuser");
    	sql->run("GRANT javadebugpriv to $newdbuser");
  }

  if ($qstyp eq "yes") {
    	sql->run("GRANT DBA to $newdbuser");
  }
#-- Now Set Datapump grants
	sql->run("GRANT IMP_FULL_DATABASE to $newdbuser");

#-- Now Set kill session grants
	sql->run("GRANT connect to $newdbuser");
	sql->run("GRANT ALTER system to $newdbuser");


}


1;

