
import os as _os

if _os.getenv("PROFILE") == "1":
    profiling_code = True

    def start_profile():
        import cProfile as _cProfile
        pr = _cProfile.Profile()
        pr.enable()
        return pr

    def end_profile(pr, results):
        pr.disable()
        import tempfile as _tempfile
        from Acquire.ObjectStore import bytes_to_string as _bytes_to_string
        t = _tempfile.mktemp()
        pr.dump_stats(t)
        with open(t, "rb") as FILE:
            data = FILE.read()
        _os.unlink(t)
        results["profile_data"] = _bytes_to_string(data)

else:
    profiling_code = False

    def start_profile():
        return None

    def end_profile(profiler, results):
        return results

__all__ = ["start_profile", "end_profile"]
