# ------------------------------------------------------
# NAME
#	Compare - Provide commands for Schema Compare
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# ------------------------------------------------------
# $log$
# ------------------------------------------------------

=head1 NAME

Compare - Provide commands for Schema Compare

=head1 RESPONSIBILITIES

=head1 COLLABORATIONS

=cut

package Compare;
use Misc;
use strict;
use Plugin;
use SessionMgr qw(sessionNr sessionMain sql);

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
	$this->{description} = qq(
Commands for comparing things in the database.
);
	bless ($this, $class);

	return $this;
}


# ------------------------------------------------------------
sub objectCompare {
	# compare two 2-elemet arrays
	my ($one, $two) = @_;
	my $a = $one->[0];
	my $b = $one->[1];
	my $c = $two->[0];
	my $d = $two->[1];

	if ($a gt $c) { return 1;}
	if ($a lt $c) { return -1;}
	# a == c:
	if ($b gt $d) { return 1;}
	if ($b lt $d) { return -1;}

	# a == c && b = d
	return 0;
}

# ------------------------------------------------------------
sub printAtCol {
	my ($column, $text) = @_;
	if ($column) {print "\t\t\t\t\t";}
	printf ("%-39s\n", initcap(substr($text,0,39)));
}
# ------------------------------------------------------------
$COMMANDS->{"compare"} =
  {
	  alias => "com",
	  description => "compare two schemas",
	  synopsis => 
	    [
	     ["pattern"],
	     ["-s", "n", 'other schemas Session Nr'],
	     ["-a", undef, 'include objects with a Dollar'],
	     ],
	      routine => \&compareSchema,
    };

sub compareSchema {
	my ($this, $argv, $opts) = @_;

	my $mySession = $Senora::SESSION_MGR->mainSession;
	my $otherSession = sessionNr($opts->{"s"});
	my ($typePattern, $namePattern) = processPattern($argv);
	my $dollarClause = "and object_name not like '%\$%'" unless $opts->{a};

	my $stmt = qq(
		     select object_type, object_name
		     from user_objects uo
		     where upper(uo.object_name) like $namePattern 
		     and upper(uo.object_type) like $typePattern 
		     $dollarClause
		     order by object_type, object_name
		     );
	my $mySth = $mySession->prepare($stmt);
	$mySth->execute;
	my $otherSth = $otherSession->prepare($stmt);
	$otherSth->execute;

	my $diffs = 0;
	my $myObject = $mySth->fetchrow_arrayref;
	my $otherObject = $otherSth->fetchrow_arrayref;

	print $mySession->identify ."\t\t\t" . $otherSession->identify."\n";
	print "------------------------------------------------------------------------------\n";
	while ($myObject->[0] or $otherObject->[0]) {
		if (
		    !$otherObject->[0] or
		    objectCompare($myObject, $otherObject) == -1
		    ) {
			printAtCol(0, $myObject->[0]."/".$myObject->[1]);
			$myObject = $mySth->fetchrow_arrayref;
			$diffs++;
			next;
		}

		if (
		    !$myObject->[0] or
		    objectCompare($myObject, $otherObject) == 1
		    ) {
			printAtCol(1,$otherObject->[0]."/".$otherObject->[1]);
			$otherObject = $otherSth->fetchrow_arrayref;
			$diffs++;
			next;
		}

		# objects match so far
		$myObject = $mySth->fetchrow_arrayref;
		$otherObject = $otherSth->fetchrow_arrayref;
	}
	print "found $diffs differences.\n";
}

# ------------------------------------------------------------
$COMMANDS->{"compareSesstats"} =
  {
	  alias => "cmpss",
	  description => "compare session statistics of two schemas",
	  synopsis => 
	    [
	     [""],
	     ["-p", "pattern", 'statname pattern'],
	     ["-o", "n", 'other schemas Session Nr'],
	     ["-s", "sids", 'this schemas sids'],
	     ["-S", "sids", 'other schemas sids'],
	     ],
	      routine => \&compareStat
    };

sub compareStat {
	my ($this, $argv, $opts) = @_;

	my $mySession = $Senora::SESSION_MGR->mainSession;
	my $otherSession = sessionNr($opts->{o});
	my $mySid = $opts->{"s"};
	my $otherSid = $opts->{"S"};

	if (! ($mySid && $otherSid)) {
		print "you must specify two SIDs\n";
		return;
	}
	my $nameClause = "and upper(name) like '%" . uc($opts->{p}) . "%'" if $opts->{p};

	my $myStmt = qq(
       		select sid, name statname , value
		from
	      		v\$sesstat s, v\$statname n
	 	where
			s.statistic# = n.statistic#
			and sid = theSid
			$nameClause
		order by name desc
	);

	# plug in the appropriate SIDs:
	my $otherStmt = $myStmt;
	$myStmt =~ s/theSid/$mySid/;
	$otherStmt =~ s/theSid/$otherSid/;

	my $mySth = $mySession->prepare($myStmt);
	$mySth->execute;
	my $otherSth = $otherSession->prepare($otherStmt);
	$otherSth->execute;

	my $diffs = 0;

	printf ("%-40s %24s %24s\n",
		"",
		$mySession->identify . '('.$mySid.')',
		$otherSession->identify . '('.$otherSid.')'
		);
	print "------------------------------------------------------------------------------------------\n";

	my $myStat = $mySth->fetchrow_arrayref;
	my $otherStat = $otherSth->fetchrow_arrayref;

	my $line=1;
	while ( $myStat or $otherStat ) {
		if ($myStat->[1] eq $otherStat->[1]) {
			# skip double zeros
			if ($myStat->[2] || $otherStat->[2]) {
				printf ("%-40s %24s %24s\n",
					substr($myStat->[1],0,39),
					commify($myStat->[2]),
					commify($otherStat->[2])
				       );
				if ($line++ % 5 == 0) {
					print "\n";
				}
			}
		} else {
			print "statnames out of sync\n";
		}
		$myStat = $mySth->fetchrow_arrayref;
		$otherStat = $otherSth->fetchrow_arrayref;
	}

}

# ------------------------------------------------------------
$COMMANDS->{"compareOptstats"} =
  {
	  alias => "cmpos",
	  description => "compare optimizer statistics of two schemas",
	  synopsis => 
	    [
	     ["pattern"],
	     ["-s", "n", 'other schemas Session Nr'],
	     ["-d", "delta", 'minimum delta in percent'],
	     ["-a", undef, 'include objects with a Dollar'],
	     ],
	      routine => \&compareStatistics,
	  man=> qq(
Compare the num_rows of user_tables and user_indexes
)
    };

sub compareStatistics {
	my ($this, $argv, $opts) = @_;

	my $mySession = $Senora::SESSION_MGR->mainSession;
	my $otherSession = sessionNr($opts->{"s"});
	my ($typePattern, $namePattern) = processPattern($argv);

	if ($typePattern =~ /%/) {
		$typePattern = "TABLE";
		print "type pattern set to $typePattern.\n";
	}

	# decide if USER_TABLES or USER_INDEXES is the one
	my $statTable = 0;
	my $objectName = 0;
	if ($typePattern =~ /TABLE/) {
		$statTable = "User_Tables";
		$objectName = "Table_Name";
	}

	if ($typePattern =~ /INDEX/) {
		$statTable = "User_Indexes";
		$objectName = "Index_Name";
	}

	unless ($statTable) {
		print "Statistics are on tables or indexes only.\n";
		return;
	}
	my $dollarClause = "and $objectName not like '%\$%'" unless $opts->{a};

	my $stmt = qq(
		     select '$typePattern', $objectName, num_rows
		     from $statTable uo
		     where upper(uo.$objectName) like $namePattern 
		     $dollarClause
		     order by 1
		     );
	my $mySth = $mySession->prepare($stmt);
	$mySth->execute;
	my $otherSth = $otherSession->prepare($stmt);
	$otherSth->execute;

	my $diffs = 0;
	my $myObject = $mySth->fetchrow_arrayref;
	my $otherObject = $otherSth->fetchrow_arrayref;

	print $mySession->identify ."\t\t\t" . $otherSession->identify."\n";
	print "------------------------------------------------------------------------------\n";
	while ($myObject->[0] or $otherObject->[0]) {
		if (
		    !$otherObject->[0] or
		    objectCompare($myObject, $otherObject) == -1
		    ) {
			printAtCol(0, $myObject->[0]."/".$myObject->[1]);
			$myObject = $mySth->fetchrow_arrayref;
			$diffs++;
			next;
		}

		if (
		    !$myObject->[0] or
		    objectCompare($myObject, $otherObject) == 1
		    ) {
			printAtCol(1, $myObject->[0]."/".$myObject->[1]);
			$otherObject = $otherSth->fetchrow_arrayref;
			$diffs++;
			next;
		}

		# objects match so far only print if NUM_ROWS differ
		printAtCol(0,$typePattern."/".$myObject->[0].":".$myObject->[1]);
		printAtCol(1,$typePattern."/".$otherObject->[0].":".$otherObject->[1]);
		$myObject = $mySth->fetchrow_arrayref;
		$otherObject = $otherSth->fetchrow_arrayref;
	}
	print "found $diffs differences.\n";
}

# ------------------------------------------------------------
$COMMANDS->{diff} =
  {
   alias => "",
   description => "show differences of two tables",
   synopsis => [  
	       ["table1 table2"],
	       ["-c", undef, 'Only print statistics'],
		],
   routine => \&diff,
   };

sub count {
    my ($stmt) = @_;
    $a = sql->runArrayref("select count(*) from ($stmt)");
    return $a->[0][0];
}

sub diff {
	my ($this, $argv, $opts) = @_;

	my $table1 = $argv->[0] or return;
	my $table2 = $argv->[1] or return;
	shift @$argv;

	if ($opts->{c}) {
	   my $total1 = count("select * from $table1");
	   my $total2 = count("select * from $table2");
	   my $missing1 = count ("select * from $table2 minus select * from $table1");
	   my $missing2 = count ("select * from $table1 minus select * from $table2");
	   printf ("%-20s missing in %s\n%-20s missing in %s\n=> %0.2f%% ok\n\n", 
	   	  "$missing1 out of $total1", $table1,
		  "$missing2 out of $total2", $table2,
		  100 - 100*($missing1+$missing2)/($total1+$total2)); 
	   return;
	}

	print "\nRows missing in $table2:";
	my $statement = qq(
			   select * from $table1
			   minus
			   select * from $table2
			   );
	sql->run($statement);
	print "\nRows missing in $table1:";
	my $statement = qq(
			   select * from $table2
			   minus
			   select * from $table1
			   );
	sql->run($statement);

}


# ------------------------------------------------------------
$COMMANDS->{"createMapping"} =
  {
	  alias => undef,
	  description => "create a mapping table which maps primary keys",
	  synopsis => 
	    [
	     ["pattern"],
	     ["-s", "n", 'other schemas Session Nr'],
	     ["-S", "srcTable", 'The table in other session'],
	     ["-D", "dstTable", 'The table in my session'],
	     ["-x", "regexp", 'Columns to exclude'],
	     ],
	      routine => \&createMapping,
    };
sub createMapping {
	my ($this, $argv, $opts) = @_;

	my $mySession = $Senora::SESSION_MGR->mainSession;
	my $otherSession = sessionNr($opts->{"s"});


	my $stmt = qq(
		select
			cc.table_name,
			cc.constraint_name,
			cc.column_name,
			tc.data_type,
			tc.data_length
		from
			user_constraints uc,
			user_cons_columns cc,
			user_tab_columns tc
		where
			uc.table_name = cc.table_name
		and	uc.owner = cc.owner
		and	tc.table_name = cc.table_name
		and	tc.column_name = cc.column_name
		and	uc.constraint_name = cc.constraint_name
		and	uc.constraint_type = 'P'
		and	uc.table_name = ?
		order by cc.position
	);

	my $srcSth = $otherSession->prepare($stmt);
	$srcSth->bind_param(1, uc($opts->{S}));
	$srcSth->execute();

	my $dstSth = $mySession->prepare($stmt);
	$dstSth->bind_param(1, uc($opts->{D}));
	$dstSth->execute();

	my $getColumns = sub {
		my ($sth, $prefix) = @_;
		my $columnSql;
		my $fkColumns=[];
		my $pkColumns=[];
		my $sep="";
		while (my $row=$sth->fetchrow_arrayref()) {
			next if $opts->{x} && $row->[2] =~ /$opts->{x}/i;
			push (@$pkColumns, $row->[2]);
			my $columnName = $prefix."_".$row->[2];
			push (@$fkColumns, $columnName);
			my $dataType = $row->[3];
			if ($dataType !~/NUMBER|DATE/) {
				$dataType = sprintf("%s(%s)", $dataType, $row->[4]);
			}
			$columnSql .= sprintf("$sep\t%-16s %s", "$columnName", $dataType);
			$sep = ",\n";
		}
		return ($columnSql, $pkColumns, $fkColumns);
	};

	my $TableName = 'MAP_'.$opts->{D};
	my ($srcColumns, $sPkColumns, $sFkColumns) = $getColumns->($srcSth, "src");
	my ($dstColumns, $dPkColumns, $dFkColumns) = $getColumns->($dstSth, "my");

	unless ($srcColumns) {
		print ("Cannot determine PK columns for source $opts->{S}\n"); 
		return;
	}
	unless ($dstColumns) {
		print ("Cannot determine PK columns for destination $opts->{D}\n"); 
		return;
	}

	my $createStmt = qq(
------------------------------------------------------------
create table $TableName (
------------------------------------------------------------
$srcColumns,
$dstColumns
);
);
	my $fkStmt = sprintf ("Alter Table $TableName add Constraint FK_MAP_%s
	Foreign Key (%s) references %s (%s) \n\ton delete cascade;\n", 
			      $opts->{D}, 
			      join(", ",@$dFkColumns), 
			      $opts->{D}, 
			      join(", ", @$dPkColumns));

	my $commentStmt = sprintf ("Comment on Table %s is\n\t'Mapping between CBDM and my %s table';\n",
				   uc($TableName), uc($opts->{D}));
	print $createStmt;
	print $fkStmt;
	print $commentStmt;
}


1;
