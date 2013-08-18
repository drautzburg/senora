=head1 NAME

SqlFormatter - Format the query results

=head1 RESPONSIBILITIES 

Format the results of SQL queries in various ways, allowing
column breaks and stuff.

=head1 COLLABORATIONS

SqlFormatter is derived from SqlSession and used in place of it.

=cut

# $Id: SqlFormatter.pm,v 1.2 2013/05/14 10:11:20 draumart Exp $

package SqlFormatter;
use strict;
use Misc;
use Settings;
use SqlSession;
use vars qw (
	     @ISA
);

@ISA=qw(SqlSession);
# ------------------------------------------------------------
sub new {
	my ($SqlFormatter, $params) = @_;
	my $class = ref($SqlFormatter) || $SqlFormatter;

	my $this = SqlSession->new ($params);
	return 0 unless $this;

	$this->{max_pagesize} = 10;
	$this->{trunc_header} = 10;
	$this->{prefetch_limit} = 50;

	$this->{truncate} = 0;
	$this->{columnSizes} = [];
	$this->{prefetched_rows} = [];
	$this->{prefetched_cnt} = 0;
	$this->{prefetched_fetched} = 0;
	$this->{linesize} =  0;
	$this->{out} = undef;
	$this->{blocksize} = undef;
	bless ($this, $class);
	return $this;
}
access('out');
# ----------------------------------------
sub addArrayref {
	# Return an arrayref pointing to new array with same elements
	my ($this, $arrayref) = @_;

	my @arr = ();
	@arr = @$arrayref;
	#print "adding ".\@arr." \n";
	$this->{prefetched_rows}->[$this->{prefetched_cnt}] = \@arr;
}

#----------------------------------------
sub getColumnSeparator {
	my ($this, $iteration)=@_;
	# get column separator
	# continued (line broken) values get a different sparator

	if (setOn("colsep")) {
		return $iteration == 0 ? '|' : ':';
	}
	if (setting->get("colsep") eq "OFF") {
		return " ";
	}
	$iteration == 0 ? setting->get("colsep") : ' ';
}

#----------------------------------------
sub formatNumber{
    	my ($nIn, $type) = @_;
	my $nOut;
	$nOut = $nIn;

	# NULLs
	if ($nIn == "") {
	   return $nIn;
	}
	# binary_double
	if ($type == 8) {
		$nOut = int($nOut);
	}

	# truncate number fields (xxx should respect column format)
	if ($type == 3) {
		$nOut =~ s/([\.\,])(..).*/$1$2/;
	}
	return $nOut;
}

#----------------------------------------
sub formatRow {
	# fit @$columns into $this->{columnSizes}
	my ($this, $columns, $iteration, $noNumberFormatting)=@_;

	my @leftovers;
	my $linesize = 0;
	my $colno=0;
	my $result = "";

	foreach my $col (@$columns) {
		my $colsep = $this->getColumnSeparator($iteration);
		my $justify;
		my $colType = $this->{sth}->{TYPE}->[$colno];

		$col = formatNumber($col, $colType) unless $noNumberFormatting;

		if ($colType == 3 or $colType==8) {
			# right justify numbers
			$justify="";
		}else {
			$justify = "-";
		}

		my $colsize=$this->{columnSizes}->[$colno];
		$colsize=$colsize ? $colsize : 16;

		# construct a printf format-string like "%-10s|"
		my $formatString = "%".$justify. "$colsize"."s$colsep";


		my $left = substr ($col,0, $colsize);
		my $right;

		# if chopped: find last word boundary
		if (length($left)<length($col)) {
			$left =~ /(.*[ _.]+)(\w*)$/;

			if ($1 && length($1) > $colsize/2) {
				$left = $1;
				$right = $2 . substr($col, $colsize);
			} else {
				# $left is ok
				$right = substr($col, $colsize);
			}
		}

		# if col contained a newline, cut it there
		if ($left =~ /\n/m) {
			$left =~ /(.*)(\n)(.*)/;
			if ($1 && $2) {
				$right = substr($left, length($1)+1) . $right;
				$left = $1;

			}
		}

		# print the column
		$result .= sprintf ($formatString, $left);

		# remember the part that was chopped off
		$leftovers[$colno] = $right;
		$colno++;
	}

	if ($this->{truncate}) {
		return ($result);
	}
	# everything formatted ?
	if (length("@leftovers") <= (@$columns-1)) {
		if ($iteration > 1) {$result .= "\n";}
		return ($result);
	}

	# format the rest
	return $result . "\n".$this->formatRow (\@leftovers, $iteration+1) ;
}

#----------------------------------------
sub printHeader {
	# a little wrapper to format the column header
	return unless setOn('heading');
	my ($this) =@_;

	my $title = initcap($this->formatRow($this->{titles},0,"noNumberFormatting"));
	print "\n$title\n";
	for (my $i=0; $i<length($title); $i++) {
		print "-";
	};
	print "\n";
}

#----------------------------------------
sub fixedSizes {
	# set sizes as defined in a "column ... format" statement
	my ($this) = @_;
	my $titles = $this->{titles} = $this->{sth}->{NAME};

	my $colno=0;
	foreach my $title (@$titles) {
		if (my $size = columnFormat->get(uc($title))) {
			$size =~ s/^a//;
			$this->{columnSizes}->[$colno] = $size;
		}
		$colno++;
	}
}
 
#----------------------------------------
sub nameSizes {
	# init {columnSizes} so titles fit in
	my ($this) = @_;
	my $titles = $this->{titles} = $this->{sth}->{NAME};

	my $colno=0;
	foreach my $title (@$titles) {
		$this->{columnSizes}->[$colno] = length($title);
		$colno++;
	}
}

 
#----------------------------------------
sub dataSizes {
	# fill an array of sizes so data fits in. 
	# xxx must clear first. A once set maximum stays effective forever
	my ($this) = @_;

	my $sizes=[];
	for (
	     $this->{prefetched_cnt}=0; 
	     $this->{prefetched_cnt} < $this->{prefetch_limit}; 
	     $this->{prefetched_cnt}++
	     ) {
		my $fields = $this->{sth}->fetchrow_arrayref or return;


		my $colno=0;
		foreach my $col (@$fields) {
			$col = formatNumber ($col, $this->{sth}->{TYPE}->[$colno]);

			$this->{columnSizes}->[$colno] = max(
					       $this->{minwitdh}, 
					       length($col), 
					       $this->{columnSizes}->[$colno]
					       );
			$colno++;
		}
		# remember the fetched rows
		# watchout: fetchrow_arrayref uses the same array on an on
		$this->addArrayref($fields);
	}
}
 

#----------------------------------------
sub fetchPrefetched {
	my ($this) = @_;

	# no prefetched rows:
	if ($this->{prefetched_cnt} == $this->{prefetched_fetched} ) {
		$this->{prefetched_cnt} = 0;
		$this->{prefetched_fetched} = 0;
		return $this->{sth}->fetchrow_arrayref;
	}

	my $row = $this->{prefetched_rows}->[$this->{prefetched_fetched}];
	$this->{prefetched_fetched}++;
	return $row;
}

#----------------------------------------
sub getSizes {
	my ($this) = @_;

	$this->{prefetched_cnt} = 0;
	$this->{prefetched_fetched} = 0;

	$this->{columnSizes} = [];
	$this->nameSizes();
	#print "\n @{$this->{columnSizes}} \n";
	$this->dataSizes();
	# could be an error during fetch
	Feedback::printFetchError($this->{sth});
	#print "\n @{$this->{columnSizes}} \n";
	$this->fixedSizes();
	#print "\n @{$this->{columnSizes}} \n";
}
#----------------------------------------
sub fetch {
	my ($this, $maxRows) = @_;

	$this->getSizes;

	my $rowNo=0;
	my $lastRow;

	while (my $row = $this->fetchPrefetched) {
		$lastRow = $row;
		$rowNo++;
		if ($maxRows && $rowNo >= $maxRows) {
			last;
		}
		if ($rowNo == 1) {$this->printHeader;}
		print $this->formatRow($row, 0)."\n";

		my $rowLimit = setting->get("rowLimit");
		if ($rowLimit  && $rowNo>= $rowLimit) {
			print "....\n";
			last;
			
		}

		my $ps = setting->get("pagesize");
		if ($ps && $rowNo % $ps == 0 ) {
			$this->printHeader;
		}
	}
	Feedback->printRowsSelected($rowNo);
	Feedback::printError ($this->{sth});
	$this->{sth}->finish if $this->{sth};
	$this->{sth}=undef;

	# return the last row
	return $lastRow;
}

# ----------------------------------------
sub blockSize {
	my ($this) = @_;
	return $this->{blocksize} if $this->{blocksize};

	my $bs = $this->runArrayref(qq(
		select bytes/blocks 
		from user_extents 
		where rownum <2
	));
	return $this->{blocksize} = $bs->[0]->[0];
}
# ----------------------------------------
sub test {
	my $s = SqlFormatter->new({connectString=>'system/manager@C28'});
	$s->run("select object_name, object_type from dba_objects
	where object_name like '%DA%'");
	$s->disconnect;
}

1;

