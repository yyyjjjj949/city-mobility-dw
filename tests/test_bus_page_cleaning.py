import importlib.util, pathlib, unittest
P=pathlib.Path(__file__).resolve().parents[1]/'tools/clean_bus_page_snapshot.py'
spec=importlib.util.spec_from_file_location('bus_clean',P);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class BusPageTimeTests(unittest.TestCase):
 def test_after_midnight_is_next_day_without_wrapping(self):
  self.assertEqual(m.parse_time('00:15⁺¹')['service_minutes'],1455)
 def test_pickup_and_setdown_restrictions(self):
  self.assertFalse(m.parse_time('18:20s')['pickup_allowed']);self.assertFalse(m.parse_time('18:20p')['setdown_allowed']);self.assertEqual(m.parse_time('00:30⁺¹s')['day_offset'],1)
 def test_blank_and_dash_are_absence_not_zero(self):
  self.assertIsNone(m.parse_time(''));self.assertIsNone(m.parse_time('–'))
 def test_invalid_or_unidentified_times_are_not_guessed(self):
  for value in ['24:00','10:60','not running','12:00abc','10:00⁺¹⁺²']:
   with self.assertRaises(ValueError):m.parse_time(value)
 def test_cleaner_has_no_network_imports(self):
  import ast
  tree=ast.parse(P.read_text(encoding='utf-8'));names={n.module.split('.')[0] for n in ast.walk(tree) if isinstance(n,ast.ImportFrom) and n.module}|{a.name.split('.')[0] for n in ast.walk(tree) if isinstance(n,ast.Import) for a in n.names}
  self.assertFalse(names&{'requests','httpx','urllib','socket','subprocess','playwright'})
if __name__=='__main__':unittest.main()
