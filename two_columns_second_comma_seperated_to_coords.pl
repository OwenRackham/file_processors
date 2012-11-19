#!/usr/bin/perl -w

use strict;
use warnings;
use DBI;
use lib '/home/rackham/modules/';
use rackham;
my $filename = $ARGV[0];
my $ont = $ARGV[1];
open FILE, "$filename" or die $!;
	#open OUTPUT, ">"."$dirto"."tabs/"."$file".".tab"; 
	while (<FILE>){
		chomp;
		my @line = split('\t',$_);
		my @commas = split(',',$line[1]);
		foreach my $entity (@commas){
			$entity =~ s/FF://g;
		print "$line[0]\t$entity\t$ont\n";
		}		
	}