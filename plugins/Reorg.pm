# ------------------------------------------------------
# NAME
#	Reorg - Provide commans for Reorganisation
# RESPONSIBILITIES
#
# COLLABORATIONS
#
# History
# ------------------------------------------------------
# $log$
# ------------------------------------------------------

=head1 NAME

Reorg - Provide commans for Reorganisation

=head1 RESPONSIBILITIES

=head1 COLLABORATIONS

=cut

package Reorg;
use strict;
use SessionMgr;
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
	$this->{description} = qq(
Commands which operate on a large number of database objects.
);

	bless ($this, $class);

	return $this;
}

# ------------------------------------------------------------
$COMMANDS->{"files"} =
  {
	  alias => "",
	  description => "print files needed for offline backup",
	  synopsis => 
	    [
	     [],
	     ["-d", "dir", "produce scripts for backup + restore"],
	     ],
	      routine => \&files,
    };

sub files {
	my ($this, $argv, $opts) = @_;

	# get controlfiles
	my $result = sql->runArrayref(
		"select name, 4000000 bytes from v\$controlfile"
	);
	# get redo logs
	my $result = sql->runArrayref(
		"select member, bytes from v\$logfile f, v\$log l 
		where l.group# = f.group#", $result
	);
	# get datafiles
	my $result = sql->runArrayref(
		"select name, bytes from v\$datafile", $result
	);

	my $dir = File::Spec->canonpath($opts->{d}) ;

	my $total = 0;
	# list files
	foreach my $row (@$result) {
		# use only backslashes
		$row->[0] =  File::Spec->canonpath($row->[0]) ;
			printf ("%-60s%8s\n",$row->[0], megaBytes($row->[1]));
			$total += $row->[1];
	}
	print "total = ". megaBytes($total) ."\n";

	# write scripts
	if ($dir) {
		my $suffix = ($^O =~ /win/) ? ".bat" : ".sh";
		my $copy = ($^O =~ /win/) ? "copy" : "cp";

		my $backupFileName = sql->service . "_backup" . $suffix;
		my $restoreFileName = sql->service . "_restore" . $suffix;

		my $backupFile = FileHandle->new(">$backupFileName");
		unless ($backupFile) {
			print "cannot open $backupFileName\n";
			return;
		}
		my $restoreFile = FileHandle->new(">$restoreFileName");
		unless ($restoreFile) {
			print "cannot open $restoreFileName\n";
			$restoreFile->close;
			return;
		}

		foreach my $row (@$result) {
			print $backupFile "$copy $row->[0] $dir\n";
			print $restoreFile "copy " 
			  . File::Spec->canonpath("$dir/$row->[0]")
			    . " $row->[0]"
			      ."\n";
		}
		flush $backupFile;
		flush $restoreFile;
		$backupFile->close;
		$restoreFile->close; 
		print "wrote $backupFileName and $restoreFileName.\n";
	} else {

	}

}

# ------------------------------------------------------------
$COMMANDS->{"rebuild_indexes"} =
  {
	  alias => "rix",
	  description => "rebuild indexes and index partitions",
	  synopsis => 
	    [
	     [],
	     ["-x", undef, 'execute right away'],
	     ["-M", "max kB", 'only rebuild if smaller than this (maxint)'],
	     ["-m", "min kB", 'only rebuild if larger than this (0)'],
	     ["-p", "pattern", 'restrict index names'],
	     ["-P", "exclude", 'exclude index names'],
	     ["-e", "kB", 'don\'t create extents smaller than this (10kB)'],
	     ["-E", "kB", 'don\'t create extents larger than this (102400kB)'],

	     ],
	      routine => \&rebuildIndexes,
    };

sub rebuildIndexes {
	my ($this, $argv, $opts) = @_;
	my $minsize = 0;
	my $maxsize = 1000000000;
	my $minExtentSize = 10;
	my $maxExtentSize = 102400;

	$minsize = $opts->{"m"} if $opts->{"m"};
	$maxsize = $opts->{"M"} if $opts->{"M"};
	$minExtentSize = $opts->{"e"} if $opts->{"e"};
	$maxExtentSize = $opts->{"E"} if $opts->{"E"};

	my $nameClause = "";
	$nameClause .= "and segment_name like upper('%$opts->{p}%')" if $opts->{p};
	$nameClause .= "and segment_name not like upper('%$opts->{P}%')" if $opts->{P};


	my $rows = sql->runArrayref (qq(
		select * from (
			select segment_name, partition_name, sum(bytes)/1024 kb
			from user_segments
			where segment_type like 'INDEX%'
			$nameClause
			group by segment_name, partition_name
			having sum(bytes)/1024 between $minsize and $maxsize
		) order by kb asc
	));
	foreach my $row(@$rows) {
		# try 10 extents
		my $index = $row->[0];
		my $partition = $row->[1] ? "partition " . $row->[1] : "";

		# compute the desired extent size
		my $size = ($row->[2]/5); # we want 5 apx. extents
		my $extentSize = 10 ** (int (log($size)/log(10)));
		if ($size/$extentSize > 3) {
			$extentSize = 5 * $extentSize;
		}
		$extentSize=$minExtentSize if ($extentSize <$minExtentSize);
		$extentSize=$maxExtentSize if ($extentSize > $maxExtentSize);

		my $stmt = sprintf (
		   "alter index %-30s rebuild %s\n\tstorage (initial %5d k next %5d k pctincrease 0) nologging",
		   $index, $partition, $extentSize, $extentSize
		    );
		print "$stmt;\n";
		if ($opts->{x}) {
			sql->run($stmt);
		}
	}
}


# ----------------------------------------
$COMMANDS->{"constraints"} =
  {
	  alias => undef,
	  description => "drop all objects matching pattern",
	  synopsis => 
	    [
	     ["command"],
	     ["-t", "tabPattern", "only operate on these tables"],
	     ["-c", "conPattern", "only operate on these constraints"],
	     ["-y", "conType", "comma separated list of constraint types (R,U,P,C,O)"],
	     ["-q", undef, "be quiet"],
	     ["-i", undef, "show disabled constraints"],
	     ],
	      routine => \&constraints
    };


sub constraints {
	my ($this, $argv, $opts) = @_;
	my $tabPattern = $opts->{t} ? uc($opts->{t}) : '%';
	my $conPattern = $opts->{c} ? uc($opts->{c}) : '%';
	my $conTypes = $opts->{y} ? uc($opts->{y}) : 'R,U,P,C,O';
	$conTypes =~ s/([A-Z])/\'\1\'/g;
	my $command = $argv->[0]." ". $argv->[1];

	if ($opts->{i}) {
		sql->run (qq(
		select status, validated, table_name, constraint_name
		from user_constraints
		where status != 'ENABLED'
	));
		return;
	};
	unless ($command =~ /able/i) {
		print "Doubtful command: '$command' \n";
		return;
	}

	my $stmt = qq(
		select table_name, constraint_name
		from user_constraints
		where table_name like '$tabPattern'
		and constraint_name like '$conPattern'
		and constraint_type in ($conTypes)
	);
	my $sth = sql->prepare($stmt);
	$sth->execute;
	while (my $row = $sth->fetchrow_arrayref) {
		my $cmd = "alter table " . $row->[0] . " $command constraint " . $row->[1];
		print "$cmd\n" unless $opts->{q};
		sql->run($cmd);
	}

}
	


# ----------------------------------------
$COMMANDS->{"dropAll"} =
  {
	  alias => undef,
	  description => "drop all objects matching pattern",
	  synopsis => 
	    [
	     ["pattern"],
	     ["-n", undef, "don't execute"],
	     ["-c", undef, "cascade"],
	     ["-q", undef, "be quiet"],
	     ],
	      routine => \&dropAll
    };


sub dropAll {
	my ($this, $argv, $opts) = @_;

	my ($typePattern, $namePattern) = processPattern($argv);

	my $stmt = qq(
		     select object_type, object_name
		     from user_objects uo
		     where upper(uo.object_name) like $namePattern and
		     upper(uo.object_type) like $typePattern 
		     and upper (uo.object_type) not like '%PARTITION'
		     and object_name not like 'BIN\$%'
		     order by object_type, object_name
		     );
	my $sth = sql->prepare($stmt);
	$sth->execute;

	my $countDrop = 0;
	while (my $row = $sth->fetchrow_arrayref) {
		$countDrop++;
		my ($type, $object) = @$row;
		my $dropStatement = "Drop $type \"$object\"";
		if ($type =~ /^TABLE/ && $opts->{"c"}) {
			$dropStatement .= " cascade constraints";
		}
		if ($opts->{"q"}) {
			my $stmt = sql->prepare ($dropStatement);
			$stmt->execute() if $stmt;
		}else {
			print "$dropStatement;\n";
			sql->run($dropStatement) unless $opts->{n};
		}
	}
	print "$countDrop objects attempted to drop.\n";
}


# ----------------------------------------
$COMMANDS->{"obfuscateTables"} =
  {
	  alias => undef,
	  description => "prefix every matching table with 'x'  and create 1:1 views",
	  synopsis => 
	    [
	     ["-n", undef, "don't execute"],
	     ["-u", undef, "undo the effect"],
	     ["-v", undef, "print statetements"],
	     ["-n", undef, "don't execute statetements"],
	     ["pattern"],
	     ],
	      routine => \&obfuscateTables,
   man => q(
	This is for verifying that no direct access to tables is made by the
	business logic. Each table is prefixes with a LOWERCASE x and a view
	will be created whose name is the unobfuscated table_name sufffixes 
	with _V.
)
    };


sub obfuscateTables {
	my ($this, $argv, $opts) = @_;

	my ($typePattern, $namePattern) = processPattern($argv);

	my $prefixed = $opts->{u} ? "like" : "not like";
	my $statement = qq 
	  (
	   select
		   table_name
	   from
		   user_tables ut
	   where
		   table_name like $namePattern and
		   table_name $prefixed 'x%'
		   );
	my $sth = sql->prepare($statement);
	$sth->execute;

	while (my $row = $sth->fetchrow_arrayref) {
		my $oldTable = $row->[0];
		my $newTable = $oldTable;
		my $view;
		if($opts->{u}) {
			$newTable =~ s/^x//;
			$view = $newTable."_V";
		} else {
			$newTable = "x".$newTable;
			$view = $oldTable."_V";
		}
		my $renameStatement = qq (rename "$oldTable" to "$newTable");
		my $viewStatement = qq (create or replace view $view as select * from "$newTable");
		if ($opts->{v}) {
			print "$renameStatement\n";
			print "$viewStatement\n";
		}
		unless ($opts->{n}) {
			sql->run($renameStatement);
			sql->run($viewStatement);
		}
	}

}


# ----------------------------------------
$COMMANDS->{"exportTable"} =
  {
	  alias => "expt",
	  description => "create an sqlldr controlfile",
	  synopsis => 
	    [
	     ["table", "the name of the table to export"],
	     ["-t", "table", "the table to insert data if different from src"],
	     ["-s", "sep", "the separator to use (default ';')"],
     ],
	      routine => \&exportTable,
   man => q(
	Produce a controlfile named table.ctl and a data file
	named table.dat that can be used with sqlldr
)
    };


sub  exportTable{
	my ($this, $argv, $opts) = @_;
	my $table = $argv->[0] or return;
	my $iTable = $opts->{t} ? $opts->{t} : $table;
	my $fd = FileHandle->new(">"."$table.ctl");
	my $sep = $opts->{"s"} ? $opts->{"s"} : ";";

	print ($fd qq(
	LOAD DATA
        INFILE '$table.dat'
        INTO TABLE $iTable
        FIELDS TERMINATED BY  '$sep'
        TRAILING NULLCOLS
));

	my $columnQuery=qq(
		select decode (rownum, 1, '   ', ' , ') ||
	       rpad (column_name, 33, ' ')      ||
	       decode (data_type,
		   'VARCHAR2', 'CHAR NULLIF ('||column_name||'=BLANKS)', 
		   'FLOAT',    'DECIMAL EXTERNAL NULLIF('||column_name||'=BLANKS)',
		   'NUMBER',   decode (data_precision, 0, 
			       'INTEGER EXTERNAL NULLIF ('||column_name||
			       '=BLANKS)', decode (data_scale, 0, 
			       'INTEGER EXTERNAL NULLIF ('||
			       column_name||'=BLANKS)', 
			       'DECIMAL EXTERNAL NULLIF ('||
			       column_name||'=BLANKS)')), 
		   'DATE',     'DATE "&dformat" NULLIF ('||column_name||'=BLANKS)', null) 
		from   user_tab_columns 
		where  table_name = upper ('$table') 
		order  by column_id
	);
	my $sth = sql->prepare($columnQuery);
	$sth->execute;
	print ($fd "\t(\n");
	while (my $row = $sth->fetchrow_arrayref) {
		print($fd "\t".$row->[0]."\n");
	}
	print ($fd "\t)\n");
	$fd->close;
	print ("wrote $table.ctl\n");


	my $fd = FileHandle->new(">"."$table.dat");
	my $sth=sql->prepare("select * from ". uc($table));
	$sth->execute;
	my $count = 0;
	while (my $row = $sth->fetchrow_arrayref) {
		my $rowdata = join($sep,@$row);
		print ($fd "$rowdata\n");
		$count++;
		if (($count%1000) == 999) {
			print "$count rows written\n";
		}
	}
	$fd->close;
	print ("wrote $table.dat ($count rows)\n");
}
1;






