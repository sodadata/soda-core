# Exceptions vs error logs

Exceptions only for connection problems.  Principle of least surprise.  Those are unrecoverable 
anyways.

During contract verification however, it makes sense to collect problems and try to continue on 
as much as possible and report all problems at the end without throwing exceptions. All contract 
parsinng and execution problems are caught en represented as errors in the ContractResult 
object.  The reason is that you might encounter execution problems for one check, but that should 
not prevent a full contract verification to fail.  The metrics and checks that still can be executed 
should still be executed and help to build the history. Imagine a single custom SQL produces a syntax 
error in the database and it's not fixed in the next 2 weeks. That should not result in no checks or 
metrics being reported on in that contract. This is to keep the blast radius of execution exceptions 
to a minimal.
