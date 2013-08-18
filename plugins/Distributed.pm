# ------------------------------------------------------
# NAME
#	Distributed - Commands for working with multiple schemas
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# ------------------------------------------------------
# $log$
# ------------------------------------------------------

=head1 NAME

Distributed - Commands for working with multiple schemas

=head1 RESPONSIBILITIES

=head1 COLLABORATIONS

=cut

package Distributed;
use strict;
use SessionMgr qw(sql sessionNr sessionMain);
use File::Spec;
use Plugin;
use Text::Wrap qw(wrap $columns $huge);
use ConIo;
use Misc;

use vars qw (
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
	# ----------------------------------------
	$this->{commands} = $COMMANDS;

	$this->{description} = qq(Commands for working with more than
one database connection at a time or to support advanced replication); 

	bless ($this, $class);

	return $this;
}

# ------------------------------------------------------------
$COMMANDS->{"refresh"} =
  {
	  description => "do a refresh",
	  synopsis => 
	    [
	     ["group"],
	     ["-G", "group", "refresh this group"],
	     ["-S", "usr.snps", "refresh this snapshot"],
	     ["-M", "method", "c=complete=full or fast"],
	     ["-s", undef, "print script only"],
	     ["-r", "rollBack", "use this rollback segment"],
	     ["-v", undef, "be verbose"],
	     ],
	      routine => \&fullRefresh
    };

sub fullRefresh {
	my ($this, $argv, $opts) = @_;
	my $snapshots;

	# refresh a group
	if (my $group=uc($opts->{G})) {
		$snapshots = sql->runArrayref(qq(
			select
				s.owner||'.'||s.name
			from
				dba_snapshots s,
				dba_rgroup r
			where
				r.refgroup = s.refresh_group
			and	upper(r.name) = '$group'
    		));
	}

	if (my $snapshot = $opts->{S}) {
		$snapshots->[0]->[0] = $snapshot;
	}
	return unless $snapshots;

	my $method="C";
	if ($opts->{M} =~ /(fast|f$)/i) {
		$method = "F";
	}


	my $i=0;
	while ( my $snap1 = $snapshots->[$i]->[0]) {
		my $rollback = "\n\t\t\trollback_seg => '$opts->{r}'," if ($opts->{r});
		my $command = qq(
		begin
		    dbms_snapshot.refresh(
			list => '$snap1',
                    	method => '$method',$rollback
                    	push_deferred_rpc => true,
                    	refresh_after_errors => true,
                    	purge_option => 1,
                    	parallelism => 0,
                    	heap_size => 0
			);
		end;);
		if ($opts->{"s"}) {
			print "$command\n/\n";
		} else {
			print "refreshing $snapshots->[$i]->[0]\n" if $opts->{v};
			sql->run($command);
		}
		$i = $i+1;
	}
}

# ------------------------------------------------------------
$COMMANDS->{"unregisterSnapshot"} =
  {
	  alias => "unrs",
	  description => "purge snapshot log and unregister (unfinished)",
	  synopsis => 
	    [
	     ["site"],
	     ["-q", undef, "print status information only"],
	     ],
	      routine => \&unregisterSnapshot
    };

sub unregisterSnapshot {
	my ($this, $argv, $opts) = @_;
	my $site = $argv->[0];

	if ($opts->{"q"}) {
		sql->run (qq(
			select snapshot_site, count(*) 
			from dba_registered_snapshots 
			group by snapshot_site));
		return;
	}

	my $snapshots = sql->runArrayref(qq(
		select name, snapshot_id
		from dba_registered_snapshots
		where snapshot_site = '$site'
	));
	foreach my $row (@$snapshots) {
		print "name=$row->[0] id=$row->[1] \n";
		sql->run(qq(
			begin
			dbms_snapshot.purge_snapshot_from_log($row->[1]);
			end;
		));
	}

}

# ------------------------------------------------------------
$COMMANDS->{"purgeDeferror"} =
  {
	  alias => "pderr",
	  description => "purge deferred transaction log",
	  synopsis => 
	    [
	     [],
	     ],
	      routine => \&purgeDeferror
    };

sub purgeDeferror {
	my ($this, $argv, $opts) = @_;
	my $site = $argv->[0];

	sql->run("select count(*) deferrors from deferror");

	my $sth = sql->prepare("select deferred_tran_id, destination from deferror") || return;
	my $count=0;
	$sth->execute;
	while (my $row = $sth->fetchrow_arrayref) {
		my $sth = sql->prepare
		  (" begin dbms_defer_sys.delete_error('$row->[0]', '$row->[1]'); end;");
		last unless $sth;
		$sth->execute;
		$count++;
		print "$count rows deleted.\n" if ($count%20 == 0);
	}


}

# ------------------------------------------------------------
$COMMANDS->{"makeSynonyms"} =
  {
	  alias => undef,
	  description => "create or drop synonyms or views",
	  synopsis => 
	    [
	     ["pattern"],
	     ["-s", "n", 'other schemas Session Nr'],
	     ["-l", "link", 'the database link to use'],
	     ["-d", undef, "create drop statements too"],
	     ["-v", undef, "create views instead of synonyms"],
	     ["-D", undef, "only create drop statements"],
	     ["-n", undef, "don't execute"],
	     ["-S", undef, "check my synonyms for slips"],
	     ["-a", undef, "allow synonyms to synonyms to my schema"],
	     ],
	      routine => \&makeSynonyms
    };

sub makeSynonyms {
	my ($this, $argv, $opts) = @_;

	my $otherSession = sessionNr($opts->{"s"}) or return;
	my ($typePattern, $namePattern) = processPattern($argv);
	
	my $link=$opts->{l};
	my $otherSchema;

	if ($opts->{S}) {
		$this->checkForSlips();
		return;
	}

	# do some checks
	if ($link) {
		$link = '@'.$link;
		$otherSchema=undef;
	}else {
		if (sessionMain->service() ne $otherSession->service()) {
			print "This is a remote Database. Specify a link !\n";
			return;
		}
		$otherSchema=$otherSession->user().".";
	}
	if (sessionMain()->identify eq $otherSession->identify) {
		print "This is the same schema !\n";
		return;
	}

	my $stmt = qq(
		     select object_name, object_type
		     from user_objects uo
		     where upper(uo.object_name) like $namePattern and
		     upper(uo.object_type) like $typePattern 
		     order by object_type, object_name
		     );
	my $oSth = $otherSession->prepare($stmt);
	$oSth->execute;

	while (my $row = $oSth->fetchrow_arrayref) {
		my $object = $row->[0];
		my $type = $row->[1];
		if ($opts->{d} or $opts->{D}) {
			my $dropStatement 
			  = $this->synonymStatement('drop', $object, $opts);
			print "$dropStatement;\n";
			sql->run($dropStatement) unless $opts->{n};
		}
		unless ($opts->{D}) {
			unless ($opts->{a}) {
				$this->isSynonymLoop($otherSession,$object, $type) 
				  and next;
			}

			my $createStatement 
			  = $this->synonymStatement('create', $object, $opts);
			$createStatement .= "$otherSchema$object$link";
			print "$createStatement;\n";
			sql->run($createStatement) unless $opts->{n};
		}

	}

}

sub synonymStatement {
	my ($this, $command, $object, $opts) = @_;
	if ($command =~ /create/i) {
		if ($opts->{v}) {
			return "CREATE VIEW $object as select * from ";
		} else {
			return "CREATE SYNONYM $object for ";
		}
	}
	if ($command =~ /drop/i) {
		if ($opts->{v}) {
			return "DROP VIEW $object";
		} else {
			return "DROP SYNONYM $object";
		}
	}

}

sub isSynonymLoop {
	my ($this, $otherSession,$object, $type)=@_;
	return unless ($type eq 'SYNONYM');

	my $myUser = uc(sql->user());

	# check if the synonym points back to my schema
	my $sth = $otherSession->prepare (qq(
		select 1
		from user_synonyms
		where synonym_name = '$object'
		and table_owner = '$myUser'
	));
	$sth->execute;
	if ($sth->fetchrow_arrayref) {
		print "-- $object points back to schema $myUser\n";
		return 1;
	}
	return 0;
}
sub checkForSlips {
	my $i;
	my $sth = sql->prepare("select synonym_name from user_synonyms");
	$sth->execute;
	while (my $row = $sth->fetchrow_arrayref) {
		$i++;
		print ".";
		if ($i%70 == 0) {
			print "\n";
		}
		my $synonym = $row->[0];
		sql->{dbh}->prepare("select 1 from $synonym where 1=2");
		my $errMsg = $DBI::errstr;
		if ($errMsg =~ /(ORA-01775|ORA-00942|ORA-00980|ORA-02019)/) {
			$errMsg =~ s/\(DBD:.*//;
			$i=0;
			printf("\n%-16s %s\n ",$synonym,$errMsg);
		}
	}
	print "\n";

}

# ------------------------------------------------------------
$COMMANDS->{"grantAll"} =
  {
	  alias => undef,
	  description => "create or revoke lots of grants",
	  synopsis => 
	    [
	     ["pattern"],
	     ["-t", "user", "grant to user"],
	     ["-f", "user", "revoke from user"],
	     ["-n", undef, "don't execute"],
	     ["-g", "grant", "use grant instead of ALL"],
	     ["-O", undef, "grant with grant option"],
	     ],
	      routine => \&grantAll
    };

sub grantAll {
	my ($this, $argv, $opts) = @_;

	my ($typePattern, $namePattern) = processPattern($argv);
	my $user;
	$user = $opts->{f} if $opts->{f};
	$user = $opts->{t} if $opts->{t};

	unless ($user) {
		print "No user specified.\n";
		return;
	}
	if ($user eq sessionMain->user()) {
		print "You are '$user' yourself.\n";
		return;
	}
	my $grant = $opts->{g} ? $opts->{g} : 'ALL';
	my $stmt = qq(
		     select object_name
		     from user_objects uo
		     where upper(uo.object_name) like $namePattern and
		     upper(uo.object_type) like $typePattern 
		     order by object_type, object_name
		     );
	my $sth = sql->prepare($stmt);
	$sth->execute;
	while (my $row = $sth->fetchrow_arrayref) {
		my $object = $row->[0];
		my $grantStatement;
		if ($opts->{f}) {
			$grantStatement =qq(revoke $grant on $object from $user);
		}
		if ($opts->{t}) {
			$grantStatement =qq(grant $grant on $object to $user);
			$grantStatement .= " with grant option" if $opts->{O};
		}
		print "$grantStatement; \n";
		sql->run($grantStatement) unless $opts->{n};
	}
}

# ------------------------------------------------------------
$COMMANDS->{"copy"} =
  {
	  alias => "cp",
	  description => "copy data from another schema(session)",
	  synopsis => 
	    [
	     ["table [where clause]"],
	     ["-s", "n", 'other schemas Session Nr'],
	     ["-l", "localTable", 'if different from remote table'],
	     ],
	man=>q(
	Copies data from a table visible for the remote session to a
	table of the local session. The table names may be
	different. May not work with exotic column types such a
	clob. When copying date, the date formats of both sessions
	must be compatible.

	You can do the same thing with a database link if you're 
	allowed to.
EXAMPLE
	copy -s1 -l xxx mailpieces where rownum <2002

	Copies 2001 rows from the remote 'mailpieces' tables to
	the local 'xxx' table.
),
	      routine => \&copyData,
    };

sub copyData {
	my ($this, $argv, $opts) = @_;

	my $mySession = $Senora::SESSION_MGR->mainSession;
	my $otherSession = sessionNr($opts->{"s"});
	my $remoteTable = $argv->[0] or return;

	# process additional cmdline clause
	splice (@$argv,0,1);
	my $clause = "@$argv";

	my $fetchSth = $otherSession->prepare(qq(
		     select * from $remoteTable $clause
	));

	$fetchSth->execute;
	my $columns=$fetchSth->{NAME};

	# create a string of bind parameters as '?'
	my $bindParams = "";
	for (my $i=1; $i<@$columns; $i++) {
		$bindParams .= " ?, ";
	}
	$bindParams .= ' ?';

	my$localTable = $opts->{l} ? $opts->{l} : $remoteTable;
	my $insertStatement =qq(
		insert into $localTable
		values ($bindParams)
	);

	my $insertSth=$mySession->prepare($insertStatement);
	my $rowCount=0;
	while (my $row = $fetchSth->fetchrow_arrayref()) {
		# bind all parameters
		for (my $i=1; $i<=@$row; $i++) {
			$insertSth->bind_param($i, $row->[$i-1]);
		}
		if ($mySession->{dbh}->errstr) {
			print $mySession->{dbh}->errstr."\n";
			return;
		}

		$insertSth->execute();
		if ($mySession->{dbh}->errstr) {
			print $mySession->{dbh}->errstr."\n";
			return;
		}
		$rowCount++;
		my $modulo = $rowCount > 1000 ? 1000 : 100;
		if (($rowCount%$modulo) == 0 ) {
			print "$rowCount rows copied\n";
		}
	}
}

1;


