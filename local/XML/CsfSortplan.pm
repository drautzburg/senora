package CsfSortplan;
use strict;
use File::Basename;
use XML::Simple;
use Data::Dumper;
use Storable qw(freeze);
use File::Find;



# ------------------------------------------------------------
sub new {
	my ($That, $sortplanFile) = @_;
	my $class = ref($That) || $That;
	my $this = {};;
	bless ($this, $class);
	#--------------------
	die "No filename given." unless $sortplanFile;
	$this->{sortplanFile} = $sortplanFile;
	$this->{sortplanXml} = undef;
	$this->{verbose} = 1;
	$this->parseXml();
	#--------------------
	return $this;
}

# ------------------------------------------------------------
sub baseName {
	my ($this) = @_;
	my $base = basename($this->{sortplanFile});
}

# ------------------------------------------------------------
sub hline {
	# print a horizontal line
	my ($this)=@_;
	print "---------------------------------------------------------\n";
}
# ------------------------------------------------------------
sub parseXml {
	my ($this) = @_;
	$this->{sortplanXml} = XMLin($this->{sortplanFile}, 
				     ForceArray => ["Destination"],
				     KeyAttr => {SortingProduct=>"name",
						 OutletGroup=>"name",
						 Sort=>"outletGroup",
						 Action=>"sortingProduct"}
				    );
	return $this->{sortplanXml};
}

# ------------------------------------------------------------
sub printTestResult {
	# print a test result in a fixed format
	# supress "ok" messages unless in verbose mode

	my ($this, $test, $result) = @_;

	if ($this->{verbose} or $result!~/ok/i) {
		printf ("\t%-64s %s\n", $test, $result);
	}
}


# ------------------------------------------------------------
sub checkEqual {
	# Shorthand routine to compare two data structures
	# prints $msg ok | mismatch

	my ($this, $msg, $n1, $n2) = @_;

	my $match;
	if ($n1!= undef && $n2 != undef) {
		$match = (freeze($n1) eq freeze($n2));
	} else {
		$msg .= " (" . ($n1 ? "*" : "none") ."/". ($n2 ? "*" : "none") . ")";
		$match = ($n1 == $n2); # both undef
	}

	$this->printTestResult ($msg,  $match ? "ok": "mismatch");

	return $match;
}

# ------------------------------------------------------------
sub getNode {
	# get a node starting with $topNode following $nodePath ("a.b.c")

	my ($this, $nodePath, $topNode) = @_;

	# by default use the entire Sortplan as topNode
	$topNode = $this->{sortplanXml} unless $topNode;

	# if there is no nodePath we're done
	return $topNode unless($nodePath);

	# otherwise position on the next node and getNode from there
	my @nodePathArr = split("\\.", $nodePath);
	my $nextNode = $nodePathArr[0];
	my $subNode = $topNode->{$nextNode};

	# Node does not exist: return undef
	unless ($subNode) {
		# print "No such Node $nodePath\n";
		return undef;
	}

	# if the path isn't exhausted yet, continue with the remainder
	# of the path
	shift (@nodePathArr);
	return $this->getNode(join(".", @nodePathArr),$subNode);
}

# ------------------------------------------------------------
sub compareWithBy {
	# compare two parsed plans by a given node

	my ($this, $otherCsfSortplan, $nodePath) = @_;

	my $n1 = $this->getNode($nodePath);
	my $n2 = $otherCsfSortplan->getNode($nodePath);
	$this->checkEqual($nodePath,$n1, $n2);
}

sub compareWith {
	# compare two plans using selected nodes
	my ($this, $otherCsfSortplan) = @_;
	$this->compareWithBy($otherCsfSortplan, "Flags");
	$this->compareWithBy($otherCsfSortplan, "SortingProducts");
	$this->compareWithBy($otherCsfSortplan, "SortingProducts.MailAttributeLine");
	$this->compareWithBy($otherCsfSortplan, "SortingProducts.SeparatorCards");
	$this->compareWithBy($otherCsfSortplan, "Actions");
	$this->compareWithBy($otherCsfSortplan, "Actions.OutletGroups");
}

# ------------------------------------------------------------
sub findSortplans {
	# traverse directory and find all CSF files
	# return arrayref of CSF files sans the root directory
	# which was passed to this function

	my ($That, $root) = @_;
	my @sortplans;

	my $collectSortplans = sub {
		my $fileName = $File::Find::name;
		if ($fileName =~ /.*CSF$/i) {
			# strip initial part of the path
			$fileName =~ s/^$root//;
			push (@sortplans, uc($fileName));
		}
	};

	find ($collectSortplans, ($root));

	return \@sortplans;
}

sub intersectUnionDiff {
	# Classmethod: Compute union, intersection and difference of
	# two arrays

	my ($That, $a1, $a2) = @_;

	my @union = my @inter = my @diff = ();
	my %count = ();

	foreach my $element (@$a1, @$a2) {
		$count{$element}++;
	};
	foreach my $element (keys %count) {
		push @union, $element;
		push @{ $count{$element} > 1 ? \@inter : \@diff }, $element;
	}
	return (\@union,\@inter,\@diff)
}

sub compareDirectories {
	# Classmethod: Compare all CSF Sortplans in and below two
	# directories

	my ($That, $root1, $root2) = @_;

	my $plans1 = CsfSortplan->findSortplans($root1);
	my $plans2 = CsfSortplan->findSortplans($root2);
	my ($union, $inter, $diff) = CsfSortplan->intersectUnionDiff($plans1, $plans2);

	# Report Sortplans which do not come in pairs
	if (@$diff) {
		$That->hline();
		print "Sortplans which are only in one of the two directories\n";
		$That->hline();
		print "\t".(join ("\n\t", @$diff) . "\n") ;
	}

	# Compare the Sortplan pairs
	foreach my $filename (@$inter) {
		$That->hline();
		print "$filename\n";
		$That->hline();
		my $p1 = CsfSortplan->new("$root1/$filename");
		my $p2 = CsfSortplan->new("$root2/$filename");
		$p1->compareWith($p2);
	}
}
# ------------------------------------------------------------
sub printProductDefinition{
	# Print the Definition (codes or SSOs) of a Product (identified by its name)

	my ($this, $name) = @_;
	$this->hline();
	print "Definition of $name \n";
	$this->hline();
	print XMLout($this->getNode("SortingProducts.SortingProduct.$name"));
}

sub printProductActions{
	# Print the Actions of a given Product (identified by its name)

	my ($this, $name) = @_;
	$this->hline();
	print "Actions of $name \n";
	$this->hline();
	print XMLout($this->getNode("Actions.Action.$name"));
}

sub printOutletGroup {
	# Print the Outlets of a given OutletGroup (identified by its name)
	my ($this, $name) = @_;
	$this->hline();
	print "Outlets in $name \n";
	$this->hline();
	print XMLout($this->getNode("Actions.OutletGroups.OutletGroup.$name"));
}

sub printProductOgr {
	# Print outletGroup about a Product

	my ($this, $name) = @_;
	# get the outletGroup
	my $ogr = $this->getNode("Actions.Action.$name.Sort.outletGroup");
	$this->printOutletGroup($ogr);
}

sub printDestination {
	my ($this, $dest) = @_;
	# Print a single Destinaion line"

	$this->hline();
	print "Destination \n";
	$this->hline();
	print XMLout($dest);
	$this->hline();
}

# ------------------------------------------------------------
sub getAllProductNames {
	my ($this, $type) = @_;
	my $products = $this->getNode("SortingProducts.SortingProduct");
	my @result;
	foreach my $p (keys(%$products)) {
		if ($this->getNode("SortingProducts.SortingProduct.$p.$type")) {
			push (@result,$p);
		}
	}
	return @result;
}

sub getAllItemProductNames {
	my ($this) = @_;
	return $this->getAllProductNames("ItemProduct");
}

sub getAllMachineProductNames {
	my ($this) = @_;
	return $this->getAllProductNames("MachineProduct");
}

# ------------------------------------------------------------
sub containsSortcode {
	# Check if a given Product contains a given Sortcode. Return Desination node
	my ($this, $productName, $sortcode) = @_;
	my $d = $this->getNode("SortingProducts.SortingProduct.$productName.ItemProduct.Destination");
	foreach my $dest(@$d) {
		if ($dest->{SortcodeHigh} == undef) {
			return $dest if ($sortcode eq $dest->{SortcodeLow});
		} else {
			return $dest if ($dest->{SortcodeLow} le $sortcode && $sortcode le $dest->{SortcodeHigh})
		}
	}
	return 0;
	;
}
# ------------------------------------------------------------
sub findSortcode {
	# Answer the names of the products which contains a sortcodes
	my ($this, $sortcode) = @_;
	my @sortingProducts = $this->getAllItemProductNames();
	foreach my $p (@sortingProducts) {
		if ($this->containsSortcode($p, $sortcode)) {
			return $p;
		}
	}
	return undef;
}

1;

#package main;

#CsfSortplan->new("A8010861.CSF")->compareWith(csfSortplan->new("A8010861x.CSF"), "Flags");
#print CsfSortplan->new("A8010861.CSF")->containsSortcode("BI3118228980166", "80010011020520");



#CsfSortplan->compareDirectories("d:/dtz/projects-current/CH_REMA/CommonSortplanFormat-neu","d:/dtz/projects-current/CH_REMA/Fachanforderungen");

#print join "\n", CsfSortplan->new("A8010861.CSF")->getAllMachineProductNames();
#print CsfSortplan->new("A8010861.CSF")->printProductDefinition("BS31189906Sendu");
