#!/usr/bin/perl -w
use strict;
use warnings;
use Getopt::Std;
use Data::Dumper;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError) ;

use DBI;

# Arguments
our($opt_a,$opt_c,$opt_s,$opt_g,$opt_w,$opt_y,$opt_e,$opt_n,$opt_m,$opt_t,$opt_h,$opt_d,$opt_o,$opt_l,$opt_f);
getopts('a:c:s:g:w:y:e:n:m:t:h:d:o:l:f');

my $usage="perl FF.pl -t FF -e FF2FF -g FF2FF_graph_path -m FANTOM5.obo";

#my $logfile=$opt_l;
if($opt_l){
#	$logfile = IO::Tee->new(">>$opt_l", \*STDOUT);
}else{
#	$logfile = IO::Tee->new(\*STDOUT);
}

my $tempdir='/tmp';
if(defined($opt_a)){
    $tempdir=$opt_a;
}

#__END__
# ----------------------------------------------------------------------------------
my $server='supfam2';
my $database='rackham';
my $username='rackham';
my $password='';

my $dsn = "DBI:mysql:$database:$server";
my $dbh = DBI->connect( $dsn, $username, $password, { RaiseError => 1 } ) or die $DBI::errstr;
my $sth;
my $table=1;
if ($table){

    # Create table: DO
	$dbh->do("DROP TABLE IF EXISTS ".$opt_t);
	my $sql='CREATE TABLE '.$opt_t.' (
	        id VARCHAR(20) NOT NULL,
            name varchar(100) NOT NULL,
            namespace varchar(100) NOT NULL,
            def text NOT NULL,
            synonym text NOT NULL,
            distance int(10) NOT NULL,
            is_leaf tinyint(1) NOT NULL,
            
            Key id (id),
            Key name (name),
            Key namespace (namespace),
            Key distance (distance),
            Key is_leaf (is_leaf)
            ) ENGINE=MyISAM DEFAULT CHARSET=latin1';
	$sth = $dbh->prepare($sql);
    $sth->execute ();
	$sth->finish ();
	
    # Create table: FF2FF
	$dbh->do("DROP TABLE IF EXISTS ".$opt_e);
	$sql='CREATE TABLE '.$opt_e.' (
	        id_parent VARCHAR(20) NOT NULL,
            id_child VARCHAR(20) NOT NULL,
            relationship VARCHAR(20) NOT NULL,
            
            Key id_parent (id_parent),
            Key id_child (id_child)
            ) ENGINE=MyISAM DEFAULT CHARSET=latin1';
	$sth = $dbh->prepare($sql);
    $sth->execute ();
	$sth->finish ();
	
    # Create table: FF2FF_graph_path
	$dbh->do("DROP TABLE IF EXISTS ".$opt_g);
	$sql='CREATE TABLE '.$opt_g.' (
	        id_parent VARCHAR(20) NOT NULL,
            id_child VARCHAR(20) NOT NULL,
            distance int(10) NOT NULL,
            
            PRIMARY KEY (id_parent,id_child,distance)
            ) ENGINE=MyISAM DEFAULT CHARSET=latin1';
	$sth = $dbh->prepare($sql);
    $sth->execute ();
	$sth->finish ();
		
}

# prior to the update, delete the content of table
$sth = $dbh->prepare( "DELETE FROM $opt_t" );
$sth->execute;
$sth->finish;
# prior to the update, delete the content of table
$sth = $dbh->prepare( "DELETE FROM $opt_e" );
$sth->execute;
$sth->finish;
# prior to the update, delete the content of table
$sth = $dbh->prepare( "DELETE FROM $opt_g" );
$sth->execute;
$sth->finish;

# delete the file if exists
if(-e $tempdir."/".$opt_t){
    delete_file($tempdir."/".$opt_t);
}
if(-e $tempdir."/".$opt_e){
    delete_file($tempdir."/".$opt_e);
}
if(-e $tempdir."/".$opt_g){
    delete_file($tempdir."/".$opt_g);
}

####################################################################################################
### for -m gene_ontology.1_2.obo
print "$opt_m\n";
my $a=0;
my $line="";
my $id="";
my $name="";
my $def="";
my $synonym="";
my $namespace="";
my $ob=0;
my $i=0;
my $flag=0;
my %mp; # hash of record: key (id), record (id), record (name), record (def), record (synonym), record (distance)
my %pc=();# pc hash: inner key (mp_parent) and outer key (mp_child), value (is_a or part_of)
my %pc_need=();# pc_need hash: inner key (mp_parent) and outer key (mp_child)
my %cp=();# cp hash: inner key (mp_child) and outer key (mp_parent)
my %cp_need=();# cp hash: inner key (mp_child) and outer key (mp_parent)
open(MPO,"$opt_m") or die "$!";
my $is_a=0; # due to the 3 subontologies (disjoint_from)
while(<MPO>){
	chomp;
	$line=$_;
	
	if($line=~/^\[Term\]/){
		$flag=1;
		
		$i=0;
		$id="";
        $name="";
        $def="";
        $synonym="";
        $namespace="";
        $ob=0;
        
        $is_a=0;
        
		next;
	}elsif(!($line)){
	    $flag=0;
        
        if(!$ob and $id){

            if($is_a!=1){ # to determine sub-ontologies (BP, MF, CC) as direct children of hypothesized root
                $pc{'root'}{$id}='is_a';
                $pc_need{'root'}{$id}='is_a';
                $cp{$id}{'root'}='is_a';
                $cp_need{$id}{'root'}='is_a';
                
                print "$id\n";
            }

            $name=~s/\'/\\\'/g;
            $def=~s/\'/\\\'/g;
            $synonym=~s/\'/\\\'/g;
            
            $name=$id if($name eq '');
            
            my $record=();
            $record->{id}=$id;
            $record->{name}=$name;
            $record->{namespace}=$namespace;
            $record->{def}=$def;
            $record->{synonym}=$synonym;
            $mp{$record->{id}}=$record;
	    }
	    
	    next;
	}
	
	if($flag==1){
	    if($line=~/^id: (\w+:\w+)/){
	        $id=$1;
	    }elsif($line=~/^name: (.*)/){
	        $name=$1;
	    }elsif($line=~/^namespace: (.*)/){
	        $namespace=$1;
	    }elsif($line=~/^def: (.*)/){
	        $def=$1;
	    }elsif($line=~/^xref: (.*)/){
	        $i++;
	        if($i==1){
	            $synonym=$1;
	        }else{
	            $synonym=$synonym."|".$1;
	        }
	    }elsif($line=~/^is_a: (\w+:\w+)/){
	        $pc{$1}{$id}='is_a';
	        $pc_need{$1}{$id}=1;
            $cp{$id}{$1}=1;
            $cp_need{$id}{$1}=1;
            
            $is_a=1;
            
        }elsif($line=~/^relationship: (\w+) (\w+:\w+)/){
	        $pc{$2}{$id}=$1;
	        $pc_need{$2}{$id}=$1;
            $cp{$id}{$2}=$1;
            $cp_need{$id}{$2}=$1;
            
            $is_a=1;

	    }elsif($line=~/^is_obsolete: /){
	        $ob=1;
	    }
	}
}
close MPO;

$id="root";
$name="root";
$def="root";
$synonym="root";
$namespace="root";
my $record=();
$record->{id}=$id;
$record->{name}=$name;
$record->{namespace}=$namespace;
$record->{def}=$def;
$record->{synonym}=$synonym;
$mp{$record->{id}}=$record;


my $tmpValues_e="";
my $count_all=0;
foreach my $parent (sort keys %pc){
    foreach my $child (sort keys %{$pc{$parent}}){
    
        $count_all++;
        $tmpValues_e.="$parent\t$child\t$pc{$parent}{$child}\n";     
                    
        if(!($count_all%1000000)){
            export_to_file($tempdir."/".$opt_e,$tmpValues_e);
                                                
            $count_all=0;
            $tmpValues_e="";
        }
        
    }    
}
if ($count_all){
    export_to_file($tempdir."/".$opt_e,$tmpValues_e);
}

if(1){
    if(-e $tempdir."/".$opt_e){
        load_to_mysql($tempdir."/".$opt_e, $opt_e);
    }    
}

#############################
my %graph_path;# hash of hash of hash: inner key (parent), inner key (child), outer key (distance), value (1)
my $k=1;
$a=0;
my $perLines=0;
#my $numLines=scalar(keys %mp); # total number of lines
my $numLines=scalar(keys %pc); # total number of lines
foreach my $term (sort keys %mp){

    if(!exists($pc{$term})){
        $graph_path{$term}{$term}{0}=1;
        next;
    }

    $a++;
    if(int($numLines/100) >0 ){
        if(!($a % int($numLines/100))){
            ++$perLines;
            my $now_string = localtime;
            print "\t$now_string:\t$numLines\tpercentile (%): ". $perLines."\n" ;
            #last;
        }
    }
    
    my %cp_tmp;
    if($term eq 'root'){
        foreach my $child (keys %cp_need){
            foreach my $parent (keys %{$cp_need{$child}}){
                $cp_tmp{$child}{$parent}=1;
            }
        }
        
        my $flag=1;
        my $k=0;
        $graph_path{$term}{$term}{$k}=1;
        $k++;
        while($flag){
            $flag=0;
            foreach my $child (keys %cp_tmp){
                foreach my $parent (keys %{$cp_tmp{$child}}){
                    
                    if(!exists($cp_tmp{$parent})){
                        delete($cp_tmp{$child}{$parent});       
                        $flag++;
                        
                        $graph_path{$term}{$child}{$k}=1;
                    }
                }
            }
            
            foreach my $child (keys %cp_tmp){
                if(!scalar(keys %{$cp_tmp{$child}})){
                    delete($cp_tmp{$child});
                    
#                    #print $logfile "$term\t$child\t$k\n";
                    
                }    
            }
            $k++;
        }
        
    }else{
        $cp_tmp{'root'}{'root'}=1;
        foreach my $child (keys %cp_need){
            foreach my $parent (keys %{$cp_need{$child}}){
                if($child ne $term){
                    $cp_tmp{$child}{$parent}=1;
                }else{
                    $graph_path{$parent}{$child}{1}=1;
                }
            }
        }
        
        my $flag=1;
        my $k=0;
        $graph_path{$term}{$term}{$k}=1;
        $k++;
        while($flag){
            $flag=0;
            
            my %dele; # hash: key ($child), value (1)
            
            foreach my $child (keys %cp_tmp){
                foreach my $parent (keys %{$cp_tmp{$child}}){
                    
                    if(!exists($cp_tmp{$parent})){
                        delete($cp_tmp{$child}{$parent});       
                        $flag++;
                        
                        $dele{$child}++;
                        
                        $graph_path{$term}{$child}{$k}=1;
                    }
                }
            }
            
            foreach my $child (keys %cp_tmp){
                if(!scalar(keys %{$cp_tmp{$child}}) or exists($dele{$child})){
                    delete($cp_tmp{$child});
                    
#                   #print $logfile "$term\t$child\t$k\n";
                    
                }    
            }
            
            $k++;
        }  
        
    }
    
   # last if ($a++==10);
}
print scalar(keys %graph_path)."\n";

# import into mysql: -g DO2DO_graph_path
my $tmpValues_g="";
$count_all=0;
foreach my $parent (sort keys %graph_path){
    foreach my $child (sort keys %{$graph_path{$parent}}){
        foreach my $dist (sort keys %{$graph_path{$parent}{$child}}){
            $count_all++;
            $tmpValues_g.="$parent\t$child\t$dist\n";     
                        
            if(!($count_all%1000000)){
                export_to_file($tempdir."/".$opt_g,$tmpValues_g);
                                                
                $count_all=0;
                $tmpValues_g="";
            }
        }
    }    
}
if ($count_all){
    export_to_file($tempdir."/".$opt_g,$tmpValues_g);
}

if(1){
    
    if(-e $tempdir."/".$opt_g){
        load_to_mysql($tempdir."/".$opt_g, $opt_g);
    }
    
}

#__END__

# Calculate the shortest distance to root
my %dist;# hash: key (id), value (shortest distance to root)
$k=1;
$a=0;
until(!(%cp)){
     
    foreach my $child (keys %cp){
        foreach my $parent (keys %{$cp{$child}}){
            
            if(!exists($cp{$parent})){
                delete($cp{$child}{$parent});
                
                if(!exists($dist{$child})){
                    $dist{$child}=$k;
                }elsif($dist{$child}>$k){
                    $dist{$child}=$k;
                }
            }
        }
    }
    $a=0;
    foreach my $child (keys %cp){
        if(!scalar(keys %{$cp{$child}})){
            delete($cp{$child});

#            #print $logfile "$child\t$k\n";
            $a++;
        }    
    }
    
    print "#FF at $k shortest distance to root: $a\n";
    
    $k++;
}

# from bottom to top: determine the order
my @ord=();
$k=0;
my $round=0;
my $num=0;
my %leaf; # hash: key (phenotype) and value (1 for leaf)
until(!(%pc)){
    $round++;
    $num=0;
    foreach my $parent (keys %pc){
        foreach my $child (keys %{$pc{$parent}}){
            if(!exists($pc{$child})){
                $ord[$k]=$child;
#                #print $logfile "$k\t$ord[$k]\n";
                $k++;
                delete($pc{$parent}{$child});
                $num++;
                
                $leaf{$child}=1 if($round==1);
            }
        }
    }
    
    foreach my $parent (keys %pc){
        if(!scalar(keys %{$pc{$parent}})){
            delete($pc{$parent});
            
        }
    }
    
    print "#FF at $round round: $num\n";
}
$ord[$k]='root';

# for namespace
my %namespace;# hash: key (id), value (namespace)
my %namespace_itself;# hash: key (id), value (namespace)
foreach my $id (sort keys %mp){
    if(!exists($dist{$id})){
        $namespace{$id}='root';# for root
    }elsif($dist{$id}==1){
        my $record=$mp{$id};
        my $tmp=$record->{name};

        $namespace{$id}=$tmp;# for namespace itself
        $namespace_itself{$id}=$tmp;# for namespace itself
    }
}
foreach my $id (sort keys %mp){
    foreach my $itself (keys %namespace_itself){
        if(exists($graph_path{$itself}{$id})){
            $namespace{$id}=$namespace_itself{$itself};
        }
    }
}

# import into mysql: -t CD
my $tmpValues_t="";
$count_all=0;
foreach my $id (sort keys %mp){
    my $record=$mp{$id};
    
    if(!exists($dist{$id})){
        $dist{$id}=0;
    }
    
    my $is_leaf=0;
    if(exists($leaf{$id})){
        $is_leaf=$leaf{$id};
    }
    
    $count_all++;
    $tmpValues_t.="$record->{id}\t$record->{name}\t$namespace{$id}\t$record->{def}\t$record->{synonym}\t$dist{$id}\t$is_leaf\n";
                
            if(!($count_all%1000000)){
                export_to_file($tempdir."/".$opt_t,$tmpValues_t);
                                                
                $count_all=0;
                $tmpValues_t="";
            }
}
$sth->finish;
if ($count_all){
    export_to_file($tempdir."/".$opt_t,$tmpValues_t);
}


if(1){
    
    if(-e $tempdir."/".$opt_t){
        load_to_mysql($tempdir."/".$opt_t, $opt_t);
    }
    
}

$dbh->disconnect() or warn $DBI::errstr;


########################################################################################################


sub export_to_file { 
    my $filename = shift;
    my $content = shift;
    
    open(OUT, ">>$filename") or die "!";
    print OUT $content;
    close OUT;
}

sub delete_file { 
    my $filename = shift;
    
    open(OUT, ">$filename") or die "!";
    close OUT;
}

sub load_to_mysql { 
    my $filename = shift;
    my $table    = shift;
    
    print "LOAD DATA LOCAL INFILE '$filename' INTO TABLE $table\n";
    $sth = $dbh->prepare( "LOAD DATA LOCAL INFILE '$filename' INTO TABLE $table;" );
    $sth->execute;
    $sth->finish;
}
