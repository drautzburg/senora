
=head1 NAME

	DataDictionary - Plugin to pull DataDictionary code

=head1 RESPONSIBILITIES

  	Read the DataDictionary and produce code that recreates
  	objects in the database. This is just a thin layer on top of
  	Richard Sutherlands DDL::Oracle package.

=head1 COLLABORATIONS 

=cut 

# $Id: DataDictionary.pm,v 1.2 2012/06/19 11:09:13 draumart Exp $

package DataDictionary;
use strict;
use SessionMgr;
use FileHandle;
use Settings;
use Plugin;
use Misc;
use Getopt::Long;
use DDL::Oracle;

Getopt::Long::Configure("no_ignore_case");
Getopt::Long::Configure("bundling");


use vars qw 
  (
   @ISA
   $COMMANDS
   $VIEW_OBJECTS
   $VIEW_SOURCE
);

@ISA = qw (
	   Plugin
);
$COMMANDS = {};
#------------------------------------------------------------
sub new {
	my ($That) = @_;
	my $class = ref($That) || $That;
	my $this = {};

	$this->{commands} = $COMMANDS;
	bless ($this, $class);
	return $this;
}
 

newSetting(
		    "ddView", 
		    "USER", "ALL|DBA", 
		    "chose between 'USER_', 'ALL_' and 'DBA_' views."
		   ) ->{alias} = "view";

#------------------------------------------------------------
sub view {
	# return "user" or "all" prefix
	my $view = setting->get("ddView");
	return $view if $view;
	return "user";
}

#------------------------------------------------------------
sub setViews {
	$VIEW_OBJECTS = view()."_objects";
	$VIEW_SOURCE = view()."_source";
}

#------------------------------------------------------------
sub ls_short {
	# short listing of matching objects
	my ($this, $namePattern, $typePattern, $opts) = @_;

	my $validClause = $opts->{i} ? "and uo.status != 'VALID'" : "--";
	my $dollarClause = $opts->{a} ? "" : 
	  qq(and uo.Object_Name not like '%\$%'
		and uo.Object_type not like '%PARTITION');
	my $javaClause = "and uo.Object_type not like 'JAVA%'" unless $opts->{j};

	my $notClause = $opts->{v} ? "and uo.Object_name not like " . glob2ora($opts->{v}) : "";

	setViews;
	my $statement = qq 
	  (
	   select
		   uo.object_type,
		   uo.object_name || decode(uo.subobject_name,NULL,'','('||uo.subobject_name||')'), 
		   uo.status
	   from
		   $VIEW_OBJECTS uo
	   where
		   upper(uo.object_name) like $namePattern and
		   upper(uo.object_type) like $typePattern 
		   $validClause
		   $dollarClause
		   $javaClause
		   $notClause
		   );

	my $sth = sql->prepare($statement);
	$sth->execute;

	my $column=0;
	while (my $row = $sth->fetchrow_arrayref) {
		printf ("%-40s",initcap($row->[0])."/".initcap(qnu($row->[1])));
		$column++;
		if (($column % 2) == 0) { print "\n"};
	}
	print "\n";
}
 
#------------------------------------------------------------
sub ls_long {
	# long listing of valid objects 
	my ($this, $namePattern, $typePattern, $opts) = @_;

	my $validClause = $opts->{i} ? "and uo.status != 'VALID'" : "";
	my $dollarClause = $opts->{a} ? "" : 
	  qq(and uo.Object_Name not like '%\$%'
		and uo.Object_type not like '%PARTITION');
	my $javaClause = "and uo.Object_type not like 'JAVA%'" unless $opts->{j};
	my $notClause = $opts->{v} ? "and uo.Object_name not like " . glob2ora($opts->{v}) : "";

	setViews;
	my $statement = qq (
		select
			uo.object_type,
			uo.object_name,
			uo.status,
--			uo.last_ddl_time,
			case uo.object_type
			when 'TABLE' then (
			     select last_analyzed from user_tables where table_name=uo.object_name)
			when 'TABLE PARTITION' then (
			     select last_analyzed from user_tab_partitions 
			     where table_name=uo.object_name
			     and partition_name = uo.subobject_name
			     )
			end case,
			case uo.object_type
			when 'TABLE' then (
			     select nvl(num_rows,-1) from user_tables where table_name=uo.object_name)
			when 'TABLE PARTITION' then (
			     select nvl(num_rows,-1) from user_tab_partitions 
			     where table_name=uo.object_name
			     and partition_name = uo.subobject_name
			     )
			end case,
			uo.subobject_name
		from
			$VIEW_OBJECTS uo
		where
			upper(uo.object_name) like $namePattern and
			upper(uo.object_type) like $typePattern 
			$validClause
			$dollarClause
		        $javaClause
			$notClause
		group by
			uo.object_type,
			uo.object_name,
			uo.status,
			uo.last_ddl_time,
			uo.subobject_name
			 );

	my $sth = sql->prepare($statement);
	$sth->execute;

	while (my $row = $sth->fetchrow_arrayref) {
		my $subobject = "(".$row->[5].")" if $row->[5];
		printf ("%-8s%-12s%8d %-12s%s\n",
			$row->[2],
			$row->[3],
			$row->[4],
			initcap($row->[0])."/".initcap(qnu($row->[1])). $subobject,
			);
		if ($opts->{C} && $row->[0] =~ "TABLE") {
			sql->run("
			select constraint_type||' Constraint' Type, constraint_name Name, status
				from user_constraints
				where table_name = '$row->[1]'
			union
			select 'Trigger' Type, trigger_name Name, status
				from user_triggers
				where table_name = '$row->[1]'

			");
			print "\n";
		}
		if ($opts->{I} && $row->[0] =~ "TABLE") {

			my $sth = sql->prepare (qq(
			select uniqueness||' Idx' Type, ui.index_name Name, status, column_name
			  from user_indexes ui, user_ind_columns ic
			  where ui.table_name = '$row->[1]'
			  and ui.index_name = ic.index_name
			  order by ui.table_name, ui.index_name, column_position
			));
			$sth->execute;

			my $lastIdx;
			my $cols;
			my $row;
			my @lrow;
			sql->{columnSizes}=[14,30,10,10];
			while ($row = $sth->fetchrow_arrayref) {
				#print "r $row->[1] i $lastIdx\n";
				if ($lastIdx && $row->[1] ne $lastIdx) {
					$cols =~ s/, *$//;
					print sql->formatRow(\@lrow,0)."($cols)\n";
					$cols = "";
				}
				@lrow = @$row[0..2];
				$cols .= lc($row->[3].", ");
				$lastIdx = $row->[1];
			}
			$cols =~ s/, *$//;
			if (@lrow) {
				print sql->formatRow(\@lrow,0)."($cols)\n";
				print "\n";
			} 

		}
	}
	print "\n";

}

#------------------------------------------------------------
$COMMANDS->{ls} =
  {
   alias => "",
   description => "list all objects matching pattern",
   synopsis => [
		["type/name"],
		["-a",undef,"include partitions and names with a dollar"],
		["-j",undef,"include java classes"],
		["-l",undef,"List validity of objects too"],
		["-C",undef,"List constraints and triggers too"],
		["-I",undef,"List indexes etc too"],
		["-i",undef,"List invalid objects only"],
		["-v","pattern","Exclude name"],
		],
   routine => \&ls,
   };
 

sub ls {
	# list objects
	my ($this, $argv, $opts) = @_;

	my ($typePattern, $namePattern) = processPattern($argv);

	if ($opts->{l} or $opts->{C} or $opts->{I}) {
		$this->ls_long ($namePattern, $typePattern, $opts);
	} else {
		$this->ls_short ($namePattern, $typePattern, $opts);
	}
}


#------------------------------------------------------------
sub matchingObjectNames {
	# pretty much like ls, but returns Array of Strings
	my ($this, $typePattern, $namePattern, $opts) = @_;

	my @names;

	my $objects = $this->matchingObjects($typePattern, $namePattern, $opts);
	while (my $obj = $objects->fetchrow_arrayref) {
		push (@names, $obj->[2]);
	}
	return @names;
}


# ----------------------------------------
$COMMANDS->{codeLine} =
  {
   alias => "cl",
   description => "pull code around line",
   synopsis => [
		["type/name"],
		["-l", "LineNo","Line number"],
		],
   routine => \&codeLine,
   };


sub codeLine {
	my ($this, $argv, $opts) = @_;
	my ($typePattern, $namePattern) = processPattern($argv);
	my $sth = sql->prepare(qq(
		select type, name, line, text from user_source
		where name like $namePattern
		and type like $typePattern
		and line between $opts->{l}-10 and $opts->{l}+10
		order by name, type, line
		));
	$sth->execute;
	my ($type, $name) = (0,0);;
	while (my $row = $sth->fetchrow_arrayref) {
	      if ($row->[0] ne $type or $row->[1] ne $name) {
	      	 printf("---------------------------------------------------------------------\n");
		 printf ("$row->[0]/$row->[1]\n");
	      	 printf("---------------------------------------------------------------------\n");
	      }
	      my $marker = $row->[2] eq $opts->{l} ? '*' : '';
	      printf ("%1s%4d %s", $marker, $row->[2], $row->[3]);
	      ($type, $name) = ($row->[0], $row->[1]);
	      
	}

}
#------------------------------------------------------------
sub matchingObjects {
	# pretty much like ls, but returns sth
	my ($this, $typePattern, $namePattern, $opts) = @_;

	my $validClause = $opts->{i} ? "and uo.status != 'VALID'" : "";
	my $dollarClause = $opts->{a} ? "" : "and uo.Object_Name not like '%\$%'";
	my $javaClause = "and uo.Object_type not like 'JAVA%'" unless $opts->{j};
	setViews;
	my $statement = qq 
	  (
	   select
		   uo.object_type,
	   	   user,
		   uo.object_name
	   from
		   $VIEW_OBJECTS uo
	   where
		   uo.object_name like $namePattern and
		   uo.object_type like $typePattern 
		   $validClause
		   $dollarClause
		   $javaClause
		   );

	my $sth = sql->prepare($statement);
	$sth->execute;
	return $sth;
}

# ----------------------------------------
sub extension {
	my ($type) = @_;
	$type = uc($type);

	if ($type =~ /PACKAGE BODY/) { return "PKB";}
	if ($type =~ /PACKAGE/) { return "PKS";}
	if ($type =~ /VIEW/) { return "VW";}
	if ($type =~ /PROCEDURE/) { return "PRC";}
	if ($type =~ /FUNCTION/) { return "FNC";}
	if ($type =~ /TRIGGER/) { return "TRG";}
	if ($type =~ /DATABASE LINK/) { return "DBL";}
	if ($type =~ /TABLE/) { return "TBL";}
	if ($type =~ /INDEX/) { return "IDX";}
	if ($type =~ /SEQUENCE/) { return "SEQ";}
	if ($type =~ /SYNONYM/) { return "SYN";}

	print "Don't know how to save a $type\n";
	return "unknown";
}

newSetting("pull_tbs", "FALSE","TRUE",
	      "Add tablespace clause to generated script");
newSetting("pull_schema", "FALSE","TRUE",
	      "Add schema prefix to generated script");
newSetting("pull_storage", "FALSE","TRUE",
	      "Add storage clause to generated script");
newSetting("pull_par", "FALSE","TRUE",
	      "Add parallel clause to generated script");

# ----------------------------------------
$COMMANDS->{pull} =
  {
   alias => "",
   description => "pull code of procedures, views and many more.",
   synopsis => [
		["type/name"],
		["-a",undef,"Pull all objects, even with a dollar"],
		["-f","file","Save result in file"],
		["-F",undef,"Save each object in individual file"],
		["-e",undef,"edit code in _editor"],
		["-q",undef,"Don't print to the screen"],
		],
   routine => \&pull,
   };


sub pull {
	my ($this, $argv, $opts) = @_;
	# pull code of package, trigger or whatever

	my ($typePattern, $namePattern) = processPattern($argv);

	my $fd;
	# prepare to write to single file
	if (my $file=$opts->{f}) {
		if (! ($fd = FileHandle->new (">$file"))) {
			print "Cannot open $file for writing\n";
			return;
		}
	}

	DDL::Oracle->configure 
	  (
	   dbh    => sql->{dbh},
	   view   => view(),
	   prompt   => 0,
	   schema   => setOn('pull_schema'),
	   storage  => setOn('pull_storage'),
	   tblspace => setOn('pull_tbs'),
	   parclause => setOn('pull_par'),
	   resize => 0,
	   heading => 0,
	   );

	my $objects = $this->matchingObjects($typePattern, $namePattern, $opts);
	while (my $obj = $objects->fetchrow_arrayref) {
		my $type = $obj->[0];
		my $owner = $obj->[1];
		my $name = $obj->[2];

		# Richard Sutherlands wonderful tool does the real work
		my $ddlType = $type;
		if ($type =~ /TABLE/) {$ddlType = "TABLE FAMILY";}
		my $gen = DDL::Oracle->new 
		  (
		   type => $ddlType,
		   list => [[$owner, $name]]
		   );
		my $code = $gen->create;

		# we don't want the Prompts from Richard
		# neither do we want any leading whitespace
		$code =~ s/^\s*//m;
		$code .= "\nshow errors\n";
		next unless $code;

		# Edit code if opted
		if ($opts->{e}) {
			my $tmpfile = "xxx_$name" . ".".extension($type);
			my $editor = define->get('_editor');
			unless ($editor) {
				# had no effect: $objects->finish;
				print "No editor.\n";
				return;
			}
			open FOO, ">$tmpfile" or die;
			print FOO $code;
			close FOO or die;
			system ("$editor $tmpfile");
		} 
		print $fd $code if $fd;
		print $code unless $opts->{q};

		# save in idividual files
		if ($opts->{F}) {
			my $filename = uc($name).".".extension($type);
			if (! (my $fd2 = FileHandle->new (">". uc($filename)))) {
				print "Cannot open $filename for writing\n";
			} else {
				print $fd2 $code;
				print "Wrote $filename\n";
				$fd2->close;
			}
		}

		next;
	}
	$fd->close if $fd;
	return 1;
}


#------------------------------------------------------------
$COMMANDS->{partitions} =
  {
   alias => "",
   description => "print partitioned tables or indexes",
   synopsis => [
		["pattern"],
		],
   routine => \&partitions,
   };
 

sub partitions {
	my ($this, $argv, $opts) = @_;
	my ($typePattern, $namePattern) = processPattern($argv);

	sql->run(qq(
		 select 'Table' type, table_name, partition_name, high_value 
		 from user_tab_partitions
		 where table_name like $namePattern
		 and 'TABLE' like $typePattern
		 union all
		 select 'Index', index_name, partition_name, high_value 
		 from user_ind_partitions
		 where index_name like $namePattern
		 and 'INDEX' like $typePattern
	  ));
}
#------------------------------------------------------------
$COMMANDS->{deps} =
  {
   alias => "",
   description => "print object dependencies",
   synopsis => [
		["name"],
		["-U",undef,"Print uses insted of dependencies"],
		["-r",undef,"List dependecies recursively"],
		],
   routine => \&deps,
   };
 

sub deps {
	# list dependencies
	my ($this, $argv, $opts) = @_;

	my ($typePattern, $namePattern) = processPattern($argv);
	my $objects = $this->matchingObjects($typePattern, $namePattern, $opts);
	$opts->{space}="    ";
	while (my $obj = $objects->fetchrow_arrayref) {
		my $type = $obj->[0];
		my $owner = $obj->[1];
		my $name = $obj->[2];
		print initcap($type)."/".initcap($name)."\n";
		if ($opts->{U}) {
			$this->uses("'".$name."'","'".$type."'",, "'".$owner."'", $opts);
		}else {
			$this->depends("'".$name."'","'".$type."'",, "'".$owner."'", $opts);
		}
	}
}

sub depends {
	my ($this, $name, $type, $owner,$opts) = @_;
	# a package body is never referenced by anything
	my $packageClause;
	if ($type =~ /PACKAGE BODY/) {
		$type = "'PACKAGE'";
		$packageClause = "and (type != 'PACKAGE BODY' or name != $name) ";
	}
	$type =~ s/ BODY//;
	my $statement=(qq(
		select
			name,
			type
		from user_dependencies
		where
			referenced_name like $name
		and	referenced_type like $type
		and	referenced_owner like $owner
		$packageClause
	));
	my $sth = sql->prepare($statement);
	return unless $sth;
	$sth->execute;

	while (my $row = $sth->fetchrow_arrayref) {
		print "$opts->{space}".initcap($row->[1])."/".initcap($row->[0])."\n";
		if ($opts->{r}) {
			my %myopts = %$opts;
			$myopts{space} .= "    ";
			$this->depends("'".$row->[0]."'", "'".$row->[1]."'", "'%'",\%myopts);
		}

	}
}
sub uses {
	my ($this, $name, $type, $owner,$opts) = @_;
	my $statement=(qq(
		select
			referenced_name,
			referenced_type
		from user_dependencies
		where
			name like $name
		and	type like $type
	));
	my $sth = sql->prepare($statement);

#	$sth->bind_param(1, $name);
#	$sth->bind_param(2, $type);
	return unless $sth;
#	print "$statement\n";
#	$sth->execute ($name, $type);
	$sth->execute;

	while (my $row = $sth->fetchrow_arrayref) {
		print "$opts->{space}".initcap($row->[1])."/".initcap($row->[0])."\n";
		if ($opts->{r}) {
			my %myopts = %$opts;
			$myopts{space} .= "    ";
			$this->uses("'".$row->[0]."'", "'".$row->[1]."'", "'%'",\%myopts);
		}
	}
}

#------------------------------------------------------------
$COMMANDS->{refs} =
  {
   alias => "",
   description => "print referential integrity",
   synopsis => [
		["table"],
		],
   routine => \&refs,
   };
 

sub refs {
	# list dependencies
	my ($this, $argv, $opts) = @_;

	my ($typePattern, $namePattern) = processPattern($argv);
	my $objects = $this->matchingObjects("'TABLE'", $namePattern, $opts);
	while (my $obj = $objects->fetchrow_arrayref) {
		my $type = $obj->[0];
		my $owner = $obj->[1];
		my $name = $obj->[2];
		print initcap($type)."/".initcap($name)."\n";
		$name = "'".$name."'";
		sql->run(qq(
		select NULL "referenced by", c2.table_name references, c1.constraint_name constraint, c1.delete_rule, c1.status
		from 
			user_constraints c1,
			user_constraints c2
		where
			c1.table_name = $name
			and c1. constraint_type ='R'
			and c1.r_constraint_name = c2.constraint_name
			and c1.r_owner = c2.owner
		union
		select c1.table_name "referenced by", NULL references, c1.constraint_name constraint, c1.delete_rule, c1.status
		from 
			user_constraints c1,
			user_constraints c2
		where
			c2.table_name = $name
			and c1. constraint_type ='R'
			and c1.r_constraint_name = c2.constraint_name
			and c1.r_owner = c2.owner

		));
	}
}

#------------------------------------------------------------
$COMMANDS->{find} =
  {
   alias => "",
   description => "find a line in functions, packages, triggers, types and views",
   synopsis => [
		["pattern"],
		["-i", undef, "search case insensitive"]
		],
   routine => \&find,
   };

sub formatMatch {
	# helper to format matches
	my ($string) = @_;
	chomp($string);
	# remove trailing whitespace
	$string =~ s/^\s*//m;
	return $string
}

sub find {
	my ($this, $argv, $opts) = @_;

	my $pattern = "'%".$argv->[0]."%'";
	my $text;
	$pattern =~ s/\*/%/;
	if ($opts->{i}) {
		$pattern = uc($pattern);
		$text = "upper(text)";
	} else {
		$text = "text";
	}

	my $sth=sql->prepare (qq(
		select name, type, line, text
		from user_source
		where $text like $pattern
	));
	$sth->execute();
	while (my $row = $sth->fetchrow_arrayref()) {
		printf ("%-32s %-14s %s\n",$row->[0]."/".$row->[2], $row->[1], formatMatch($row->[3]));
	}
	$this->findInLongs('VIEW', $argv->[0], $opts)
}


#------------------------------------------------------------
sub findInLongs()
  {
	  my ($this, $type, $match, $opts) = @_;

	  my $objects = $this->matchingObjects(qq('$type'), qq('%'), {});
	  $match = '^.*'.$match.'.*$';
	  # print "$match\n";
	  DDL::Oracle->configure 
	      (
	       dbh    => sql->{dbh},
	       view   => view(),
	       prompt   => 0,
	       schema   => 0,
	       storage  => 0,
	       tblspace => 0,
	       parclause => 0,
	       resize => 0,
	       heading => 0,
	   );

	  while (my $obj = $objects->fetchrow_arrayref) {
		  my $type = $obj->[0];
		  my $owner = $obj->[1];
		  my $name = $obj->[2];
		  my $gen = DDL::Oracle->new 
		    (
		     type => $type,
		     list => [[$owner, $name]]
		    );
		  my $code = $gen->create;

		  if ((! $opts->{i} && ($code =~ /($match)/m)) 
		      ||
		      ($opts->{i} && ($code =~ /($match)/mi)) 
		     )
		    {
			    my $found = $1;
			    printf "%-32s %-14s %s\n",$name, $type,formatMatch($found);
		    }
	  }
  }
#------------------------------------------------------------
$COMMANDS->{recompile} =
  {
   alias => "rec",
   description => "recompile objects",
   synopsis => [
		["type/name"],
		["-a",undef,"Do all objects, even with a dollar"],
		["-v",undef,"print statement befor executing"],
		["-n",undef,"don't execute, just show"],
		["-A",undef,"Do valid objects too"],
		],
   man => qq(
	Recompile invalid objects (or all objects).
	Will only touch PACKAGEs, VIEWs, FUNCTIONs,
	PROCEDUREs, TRIGGERs and SYNONYMs),
   routine => \&recompile,
   };

sub recompile {
	my ($this, $argv, $opts) = @_;

	my ($typePattern, $namePattern) = processPattern($argv);

	# matching objects wants -i for invalid objects
	unless ($opts->{A}) {
		$opts->{i}=1;
	}
	my $objects = $this->matchingObjects($typePattern, $namePattern, $opts);
	while (my $obj = $objects->fetchrow_arrayref) {
		my $type = uc($obj->[0]);
		my $owner = uc($obj->[1]);
		my $name = uc($obj->[2]);

		my $body;
		if ($type =~ "BODY") {
			$type="PACKAGE";
			$body="BODY";
		}
		if ($type =~ /PACKAGE|VIEW|FUNCTION|PROCEDURE|TRIGGER|SYNONYM/) {
			my $stmt = "alter $type $name compile $body";
			if ($opts->{n} or $opts->{v}) {
				print ("$stmt;\n");
			}
			unless ($opts->{n}) {
				sql->run($stmt);
			}
		}

	}
}


#------------------------------------------------------------
$COMMANDS->{objectSummary} =
  {
   alias => "osum",
   description => "count the different types of objects",
   synopsis => [],
   routine => \&objectSummary,
   };

sub objectSummary {
	my ($this, $argv, $opts) = @_;

	sql->run(qq(
		select count(*) user_objects
		from user_objects
	));

	sql->run(qq(
		select object_type, count(*)
		from user_objects
		group by object_type
	));
	sql->run(qq(
		select count(*) "functions and procedures"
		from user_objects
		where object_type in ('FUNCTION', 'PROCEDURE')
	));

	sql->run(qq(
		select constraint_type, count(*)
		from user_constraints
		group by constraint_type
	));

}

1;



