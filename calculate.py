#
# calculate
#
#
#
#
#
#

import os
import sys
import simplejson
import datetime
import MySQLdb
import getpass
import collections
from MySQLdb import cursors
import csv
import argparse

#
#
# defines
#
#


DBHOST = 'localhost'
DBUSER = 'root'
DBPASS = None
DBNAME = 'textprizm'


#
# XXX: TODO -- modify to ignore bert lines entirely?
# 
SELECT_INSTANCES_QUERY = """select dp.id as message_id, dp.participant_id as participant, dp.time as message_time, ci.user_id as user_id, ci.code_id as code_id, ci.added as date_added, ci.id as code_instance_id from data_points dp
inner join coding_instances ci on ci.message_id = dp.id
inner join coding_codes cc on cc.id = ci.code_id
where cc.schema_id = 2 and cc.id not in (117,118,119,120,121,122,123,124,126) and dp.participant_id not in (1,2)
order by dp.time asc"""




#
# helper functions
#
def pretty(obj):
	return simplejson.dumps(obj, sort_keys=True, indent=2)



#
#
# Check Args
#
#

	# add args
parser = argparse.ArgumentParser()
parser.add_argument("--maxsegtime", type=int, help="maximum segment time size", default=5)
parser.add_argument("--maxlines", type=int, help="maximum number of lines in a segment", default=5)
parser.add_argument("--binsize", type=int, help="number of datapoints to bin together", default=5)
parser.add_argument("--minbinentries", type=int, help="minimum threshold of datapoints per bin", default=5)
parser.add_argument("--maxbinskip", type=int, help="maximum number of lines in a bin", default=50 )
parser.add_argument("--dbhost", help="Database host name", default=DBHOST)
parser.add_argument("--dbuser", help="Database user name", default=DBUSER)
parser.add_argument("--dbname", help="Database name", default=DBNAME)
parser.add_argument("outfile", type=str, help="filename for the overall agreement data")
parser.add_argument("user_agreement_file", type=str, help="filename for the user agreement data")


	# parse args
args = parser.parse_args()




	# grab db password
if DBPASS is None:
	DBPASS = getpass.getpass('enter database password: ')


#============================================================================
#
# Classes
#
#============================================================================

#
# ETCRow
#
class ETCRow:
	'''Wrapper around the database row'''
	def __init__(self, row):
		self.id = row[0]
		self.participant_id = row[1]
		self.time = row[2]
		self.user_id = row[3]
		self.code_id = row[4]
		self.code_added_time = row[5]
		self.code_instance_id = row[6]

	def __str__(self):
		return "ETCRow( id:%d, pid: %d, time: %s, user: %d, code: %d, added: %s, instance_id: %s)"%(
			self.id,
			self.participant_id,
			self.time.isoformat(' '),
			self.user_id,
			self.code_id,
			self.code_added_time.isoformat(' '),
			self.code_instance_id)


#
# ETCUserCodes
#
#	A collection of a single user's codes for a specific datapoint
#
class ETCUserCodes:
	'''Holds a set of codes for the user'''
	def __init__(self, row):
		self.id = row.user_id
		self.name = None
		self.codes = {}
		self.Add(row.code_id, row.code_added_time)

	def Add(self,code,time):
		# add to the code_set
		if code not in self.codes:
			self.codes[code] = [time]
		else:
			self.codes[code].append(time)


#
# ETCDatapoint
#
#	A single datapoint from the ETC dataset
#
class ETCDatapoint:
	'''An individiual line with all codes from the users'''
	def __init__(self, row):
		self.id = row.id
		self.participant_id = row.participant_id
		self.time = row.time
		self.users = {}

		self.users[row.user_id] = ETCUserCodes(row)


	def AddRow(self,row):
		if row.user_id not in self.users:
			self.users[row.user_id] = ETCUserCodes(row)
		else:
			self.users[row.user_id].Add(row.code_id, row.code_added_time)


#
# Segment
#
# 	A segment of several datapoints
#
class Segment:
	''' A container for a segment of datapoints'''
	def __init__(self, dp):
		self.datapoints = [dp]
		self.time = dp.time
		self.max_time = dp.time
		self.participant_id = dp.participant_id


	def AddDatapoint(self,dp):
		if len(self.datapoints) == 0:
			self.time = dp.time

		if dp.time > self.max_time:
			self.max_time = dp.time

		self.datapoints.append(dp)


	def __str__(self):
		return "Segment for %s- %s by %d - datapoints: %d"%(self.time.isoformat(' '), self.max_time.isoformat(' '),self.participant_id, len(self.datapoints))


#
# TimeSegmenter
#
#	Segments the datapoints within a specified time range and by participant
#
class TimeSegmenter:
	'''A class for segmenting the data'''
	def __init__(self, time_threshold = 5):
		self.segments = []
		self.active_segments = []
		self.current_time = datetime.datetime.min
		self.time_threshold = time_threshold



	def PruneSegments(self):
		#print "active segments: %d"%(len(self.active_segments))
		while len(self.active_segments) > 0:
			time_delta = abs(self.current_time - self.active_segments[0].time)
			#print "%s - %s = %d"%(self.current_time, self.active_segments[0].time, time_delta.total_seconds())
			if time_delta is not None and time_delta.total_seconds() > self.time_threshold:
				dp = self.active_segments.pop(0)
				self.segments.append(dp)
			else:
				break



	def FindActiveSegment(self, dp):
		for s in self.active_segments:
			if s.participant_id == dp.participant_id:
				return s



	def AddDatapoint(self, dp):
		self.current_time = dp.time

		# first remove any aged out segments
		self.PruneSegments()

		# try to find a segment and append it
		s = self.FindActiveSegment(dp)
		if s is not None:
			s.AddDatapoint(dp)
		else:
			s = Segment(dp)
			self.active_segments.append(s)


#
# Segmenter
# 
#	Segments the datapoints by a number of datapoints
#
class Segmenter:
	'''Segment data by having X datapoints'''
	def __init__(self, line_threshold = 5):
		self.segments = []
		self.active_segment = None
		self.current_id = -1
		self.line_threshold = line_threshold

	def PruneSegments(self):
		#print "active segments: %d"%(len(self.active_segments))
		if self.active_segment is not None:
			if len(self.active_segment.datapoints) >= self.line_threshold:
				self.segments.append(self.active_segment)
				self.active_segment = None



	def AddDatapoint(self, dp):
		self.current_time = dp.time

		# first remove any aged out segments
		self.PruneSegments()

		# try to find a segment and append it
		if self.active_segment is not None:
			self.active_segment.AddDatapoint(dp)
		else:
			self.active_segment = Segment(dp)


#
# Agreement Calculator
#
#	Calculates agreement per pair across all datapoints in the segment
#
class AgreementCalculator:
	'''Class to caclulate the agreement'''
	def __init__(self, segments):
		self.segments = segments


	def BuildSegmentSets(self, segment):
		users = {}

		# step through datapoints
		for dp in segment.datapoints:
			# from each datapoint, grab the userid and codes and join them
			for id, codes in dp.users.items():
				if id not in users:
					users[id] = set()
				users[id] |= set(codes.codes.keys())

		# return the results
		return users

	def CalculateSegmentAgreement(self, users):
		pairs = {}
		numusers = len(users)
		for i in range(0,numusers):
			for j in range(i+1,numusers):
				user_ids = [users.keys()[i], users.keys()[j]]
				user1 = min(user_ids)
				user2 = max(user_ids)
				pair_id = "%d-%d"%(user1,user2)

				# agreed codes is the intersection of the two
				agreed_codes = len(set(users[user1] & users[user2]))

				# disagreed codes are the symmetric difference
				disagreed_codes = len(set(users[user1] ^ users[user2]))

				#print "agreed: %d | disagreed: %d"%(agreed_codes,disagreed_codes)

				if (agreed_codes + disagreed_codes) > 0:
					# calculate the total agreement
					pct_agreement = float(agreed_codes) * 100.0 / float(agreed_codes + disagreed_codes)

					# add to our dictionary
					pairs[pair_id] = pct_agreement
		return pairs



	def CalcAgreementBySegments(self):
		results = []
		for s in self.segments:
			users = self.BuildSegmentSets(s)
			pairs = self.CalculateSegmentAgreement(users)

			#print users
			#print users.keys()
			#print pairs
			#print pairs.values()
			user_ids = ' '.join(str(v) for v in users.keys())
			pct_agreements = ', '.join(str(v) for v in pairs.values())

			row = {'id': s.datapoints[0].id, 'time': s.datapoints[0].time}
			#print row
			#print pairs
			row.update(pairs)
			results.append(row)
			#print "%s:%s"%(user_ids, pct_agreements)
		return sorted(results, key=lambda x: x['id'])


class UserAgreementCalculator:
	'''Class to caclulate the user agreement'''
	def __init__(self, segments):
		self.segments = segments


	def BuildSegmentSets(self, segment):
		users = {}

		# step through datapoints
		for dp in segment.datapoints:
			# from each datapoint, grab the userid and codes and join them
			for id, codes in dp.users.items():
				if id not in users:
					users[id] = set()
				users[id] |= set(codes.codes.keys())

		# return the results
		return users

	def CalculateSegmentAgreement(self, users):
		user_agreements = {}
		numusers = len(users)
		for i in range(0,numusers):
			for j in range(i+1,numusers):
				user_ids = [users.keys()[i], users.keys()[j]]
				user1 = min(user_ids)
				user2 = max(user_ids)

				# agreed codes is the intersection of the two
				agreed_codes = len(set(users[user1] & users[user2]))

				# disagreed codes are the symmetric difference
				disagreed_codes = len(set(users[user1] ^ users[user2]))

				#print "agreed: %d | disagreed: %d"%(agreed_codes,disagreed_codes)

				if (agreed_codes + disagreed_codes) > 0:
					# calculate the total agreement
					pct_agreement = float(agreed_codes) * 100.0 / float(agreed_codes + disagreed_codes)

					# add to our dictionary
					if user1 not in user_agreements:
						user_agreements[user1] = [pct_agreement]
					else:
						user_agreements[user1].append(pct_agreement)

					if user2 not in user_agreements:
						user_agreements[user2] = [pct_agreement]
					else:
						user_agreements[user2].append(pct_agreement)

		user_agreement = {}
		for k,v in user_agreements.items():
			user_agreement[k] = float(sum(v))/len(v) if len(v) > 0 else None

		return user_agreement



	def CalcAgreementBySegments(self):
		results = []
		for s in self.segments:
			users = self.BuildSegmentSets(s)
			pairs = self.CalculateSegmentAgreement(users)

			#print users
			#print users.keys()
			#print pairs
			#print pairs.values()
			user_ids = ' '.join(str(v) for v in users.keys())
			pct_agreements = ', '.join(str(v) for v in pairs.values())

			row = {'id': s.datapoints[0].id, 'time': s.datapoints[0].time}
			#print row
			#print pairs
			row.update(pairs)
			results.append(row)
			#print "%s:%s"%(user_ids, pct_agreements)
		return sorted(results, key=lambda x: x['id'])




class CodeAgreementCalculator:
	'''Class to caclulate the code agreement'''
	def __init__(self, segments, code):
		self.segments = segments


	def BuildSegmentSets(self, segment):
		users = {}

		# step through datapoints
		for dp in segment.datapoints:
			# from each datapoint, grab the userid and codes and join them
			for id, codes in dp.users.items():
				if id not in users:
					users[id] = set()
				users[id] |= set(codes.codes.keys())

		# return the results
		return users


	def CalculateSegmentAgreement(self, users):
		user_agreements = {}
		numusers = len(users)
		for i in range(0,numusers):
			for j in range(i+1,numusers):
				user_ids = [users.keys()[i], users.keys()[j]]
				user1 = min(user_ids)
				user2 = max(user_ids)

				# agreed codes is the intersection of the two
				agreed_codes = len(set(users[user1] & users[user2]))

				# disagreed codes are the symmetric difference
				disagreed_codes = len(set(users[user1] ^ users[user2]))

				#print "agreed: %d | disagreed: %d"%(agreed_codes,disagreed_codes)

				if (agreed_codes + disagreed_codes) > 0:
					# calculate the total agreement
					pct_agreement = float(agreed_codes) * 100.0 / float(agreed_codes + disagreed_codes)

					# add to our dictionary
					if user1 not in user_agreements:
						user_agreements[user1] = [pct_agreement]
					else:
						user_agreements[user1].append(pct_agreement)

					if user2 not in user_agreements:
						user_agreements[user2] = [pct_agreement]
					else:
						user_agreements[user2].append(pct_agreement)

		user_agreement = {}
		for k,v in user_agreements.items():
			user_agreement[k] = float(sum(v))/len(v) if len(v) > 0 else None

		return user_agreement



	def CalcAgreementBySegments(self):
		results = []
		for s in self.segments:
			users = self.BuildSegmentSets(s)
			pairs = self.CalculateSegmentAgreement(users)

			#print users
			#print users.keys()
			#print pairs
			#print pairs.values()
			user_ids = ' '.join(str(v) for v in users.keys())
			pct_agreements = ', '.join(str(v) for v in pairs.values())

			row = {'id': s.datapoints[0].id, 'time': s.datapoints[0].time}
			#print row
			#print pairs
			row.update(pairs)
			results.append(row)
			#print "%s:%s"%(user_ids, pct_agreements)
		return sorted(results, key=lambda x: x['id'])





def avg(v):
	return (sum(v) / float(len(v)))

class AverageAggregator:
	'''Aggregates several segments into one value'''
	def __init__(self, agreement_data, bin_size=10, cull_pairs_under_threshold=10, maxskip=10):
		self.agreement_data = agreement_data
		self.bin_size = bin_size
		self.cull_threshold = cull_pairs_under_threshold
		self.maxskip = maxskip

	def average(self,lines):
		pairs = {}
		results = {}
		for l in lines:
			line_pairs = {p:v for p,v in l.items() if p != 'id' and p != 'time'}
			#print "line_pairs: ", pretty(line_pairs)
			for lp,v in line_pairs.items():
				if lp not in pairs:
					pairs[lp] = [v]
				else:
					pairs[lp].append(v)
		# average the pairs together
		results = {k:avg(v) for (k, v) in pairs.items() if len(v) > self.cull_threshold }
		#print "results: ", pretty(results)
		return results





	def bin(self):
		results = []
		cur_bin = { 'min-id': sys.maxint, 'max-id': -sys.maxint-1, 'lines': [] }
		for i in self.agreement_data:
			if len(cur_bin['lines']) > self.bin_size:
				aggregate = self.average(cur_bin['lines'])

				aggregate.update({'id': cur_bin['min-id'], 'time': cur_bin['lines'][0]['time'] })
				#print pretty(aggregate)
				results.append(aggregate)
				#reset the bin
				cur_bin = { 'min-id': sys.maxint, 'max-id': -sys.maxint-1, 'lines': [] }

			if cur_bin['min-id'] != sys.maxint and (i['id'] - cur_bin['min-id']) >= self.maxskip:
				print "maxskip(%d) exceeded (%d,%d)"%(self.maxskip, i['id'], cur_bin['min-id'])
				cur_bin = { 'min-id': i['id'], 'max-id': i['id'], 'lines': [i] }
			else:
				cur_bin['lines'].append(i)
				cur_bin['min-id'] = min(cur_bin['min-id'], i['id'])
				cur_bin['max-id'] = max(cur_bin['max-id'], i['id'])

		return results


#
#
# global vars
#
#

data_points = {}
unique_users = set()



# ================================================================
#
#
# begin main
#
#
# ================================================================


print "Connecting to db... (%s@%s %s)"%(args.dbuser,args.dbhost, args.dbname)
db = MySQLdb.connect(host=args.dbhost, user=args.dbuser, passwd=DBPASS, db=args.dbname, charset='utf8', use_unicode=True)
cursor = db.cursor(cursors.SSCursor)

cursor.execute(SELECT_INSTANCES_QUERY)
cnt = 0
dbrow = cursor.fetchone() 
while dbrow is not None: # and cnt < 5:
    # process

    row = ETCRow(dbrow)
    unique_users.add(row.user_id)
    
    if row.id not in data_points:
    	data_points[row.id] = ETCDatapoint(row)
    else:
    	dp = data_points[row.id]
    	dp.AddRow(row)

    dbrow = cursor.fetchone()
    cnt+=1


print "Segmenting... (maxtime=%d, maxlines=%d)"%(args.maxlines, args.maxlines)
segmenter = Segmenter(args.maxlines)

for dp,k in data_points.items():
	if len(k.users) > 1:
		#print dp, k
		segmenter.AddDatapoint(k)


print "%d segments"%(len(segmenter.segments))
maxseg = 0
minusers = 0
maxusers = 0
for dp in segmenter.segments:
	maxseg = max(maxseg, len(dp.datapoints))
	#print dp
	#minusers = min(minusers, dp.users)

print "max segment length: %d msgs"%maxseg

#user_matrix = dict(zip(sorted(unique_users), [None] * len(unique_users)))
#for k,v in unique_users.items():
#	unique_users[]
#print pretty(user_matrix)

agreementcalc = AgreementCalculator(segmenter.segments)
results = agreementcalc.CalcAgreementBySegments()
#print pretty([r['id'] for r in results])

print "aggregating %d lines with %d minimum values to be included"%(args.binsize, args.minbinentries)

aggr = AverageAggregator(results,args.binsize, args.minbinentries, args.maxbinskip)
results = sorted(aggr.bin(), key=lambda x: x['id'])

# write out basic pair agreement
fieldnames = ['id','time']
for i in range(1,22):
	for j in range(i+1, 22):
		fieldnames.append('%d-%d'%(i,j))

data_file = open(args.outfile, "wt")
csvwriter = csv.DictWriter(data_file, delimiter=",", fieldnames=fieldnames)
csvwriter.writerow(dict((fn,fn) for fn in fieldnames))
for row in results:
	#
	#print row
	csvwriter.writerow(row)


# write out basic user agrement
useragreementcalc = UserAgreementCalculator(segmenter.segments)
results = useragreementcalc.CalcAgreementBySegments()

aggr = AverageAggregator(results,args.binsize, args.minbinentries, args.maxbinskip)
results = sorted(aggr.bin(), key=lambda x: x['id'])

fieldnames = ['id','time']
for k in range(1,22):
	fieldnames.append(k)

data_file = open(args.user_agreement_file, "wt")
csvwriter = csv.DictWriter(data_file, delimiter=",", fieldnames=fieldnames)
csvwriter.writerow(dict((fn,fn) for fn in fieldnames))
for row in results:
	csvwriter.writerow(row)





